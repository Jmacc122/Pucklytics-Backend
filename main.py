"""
Pucklytics — FastAPI entry point.

Starts the slow-mode scheduler on startup and exposes REST endpoints
for the frontend to consume.
"""

import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone, date as PyDate
from zoneinfo import ZoneInfo

import httpx
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

import database
from models import GameState, TiltResponse, TiltRecord, TiltEvent, HealthResponse
from scheduler import create_scheduler, fetch_schedule, _ensure_fast_mode

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

_scheduler = create_scheduler()

MT = ZoneInfo("America/Denver")


def _mt_today() -> PyDate:
    return datetime.now(MT).date()


@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Starting up...")
    try:
        await database.init_db()
        _scheduler.start()
    except Exception as exc:
        print(f"Startup failed: {exc}")
        raise
    yield
    # Shutdown
    _scheduler.shutdown(wait=False)
    await database.close_db()


app = FastAPI(title="Pucklytics API", version="1.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://pucklytics2.vercel.app", "http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

NHL_BOXSCORE_URL = "https://api-web.nhle.com/v1/gamecenter/{game_id}/boxscore"


async def _fetch_boxscore(client: httpx.AsyncClient, game_id: int) -> dict:
    """Fetch the boxscore for a finished game. Returns empty dict on failure."""
    try:
        resp = await client.get(NHL_BOXSCORE_URL.format(game_id=game_id), timeout=10.0)
        resp.raise_for_status()
        return resp.json()
    except Exception as exc:
        logging.getLogger(__name__).warning("Boxscore fetch failed for %s: %s", game_id, exc)
        return {}


def _boxscore_to_game_state(api_game: dict, boxscore: dict, db_row: dict | None) -> GameState:
    """
    Build a GameState for a past game by merging schedule + boxscore + DB data.

    Boxscore is authoritative for final scores and SOG.
    DB supplies en_goals and tilt data accumulated during live tracking.
    """
    home = boxscore.get("homeTeam", {})
    away = boxscore.get("awayTeam", {})
    return GameState(
        game_id=api_game["game_id"],
        home_team=home.get("abbrev") or api_game["home_team"],
        away_team=away.get("abbrev") or api_game["away_team"],
        home_score=home.get("score", 0),
        away_score=away.get("score", 0),
        period=boxscore.get("periodDescriptor", {}).get("number", 0),
        time_remaining="Final",
        game_state=boxscore.get("gameState", api_game["game_state"]),
        strength="evenStrength",
        empty_net="none",
        home_sog=home.get("sog", 0),
        away_sog=away.get("sog", 0),
        en_goals=(db_row.get("en_goals", 0) or 0) if db_row else 0,
        start_time_utc=boxscore.get("startTimeUTC") or api_game.get("start_time_utc"),
        win_probability=None,
        updated_at=db_row["updated_at"] if db_row else datetime.now(timezone.utc),
    )


def _row_to_game_state(row: dict) -> GameState:
    return GameState(
        game_id=row["game_id"],
        home_team=row["home_team"],
        away_team=row["away_team"],
        home_score=row["home_score"],
        away_score=row["away_score"],
        period=row["period"],
        time_remaining=row["time_remaining"],
        game_state=row["game_state"],
        strength=row["strength"],
        empty_net=row["empty_net"],
        home_sog=row.get("home_sog", 0) or 0,
        away_sog=row.get("away_sog", 0) or 0,
        en_goals=row.get("en_goals", 0) or 0,
        start_time_utc=row.get("start_time_utc") or None,
        win_probability=row.get("win_probability"),
        updated_at=row["updated_at"],
    )


def _default_game_state(api_game: dict) -> GameState:
    """Build a GameState with default values for a game not yet in the DB."""
    return GameState(
        game_id=api_game["game_id"],
        home_team=api_game["home_team"],
        away_team=api_game["away_team"],
        home_score=0,
        away_score=0,
        period=0,
        time_remaining="",
        game_state=api_game["game_state"],
        strength="evenStrength",
        empty_net="none",
        start_time_utc=api_game.get("start_time_utc"),
        win_probability=None,
        updated_at=datetime.now(timezone.utc),
    )


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.get("/admin/migrate")
async def run_migration():
    """One-time migration: add period and time_remaining columns to tilt_history."""
    async with database.get_pool().acquire() as conn:
        await conn.execute(
            "ALTER TABLE tilt_history ADD COLUMN IF NOT EXISTS period INTEGER NOT NULL DEFAULT 0"
        )
        await conn.execute(
            "ALTER TABLE tilt_history ADD COLUMN IF NOT EXISTS time_remaining TEXT NOT NULL DEFAULT ''"
        )
    return {"migrated": True}


@app.get("/health", response_model=HealthResponse)
async def health():
    return HealthResponse(status="ok")


@app.get("/test/schedule")
async def test_schedule():
    """
    Manually trigger a schedule check and return what the NHL API reports.
    Does NOT spin up fast-mode trackers — read-only diagnostic endpoint.
    """
    try:
        games = await fetch_schedule()
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"NHL API error: {exc}")

    return {
        "game_count": len(games),
        "games": games,
    }


@app.get("/test/tracker")
async def test_tracker():
    """
    Spin up fast-mode trackers for ALL live or upcoming games today.
    For testing only — safe to call repeatedly (_ensure_fast_mode is idempotent).
    """
    try:
        games = await fetch_schedule()
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"NHL API error: {exc}")

    started = []
    for game in games:
        if game["game_state"] not in {"OFF", "FINAL"}:
            _ensure_fast_mode(game["game_id"])
            started.append(game["game_id"])

    if not started:
        raise HTTPException(status_code=404, detail="No active or upcoming games found")

    return {"started": True, "game_ids": started, "count": len(started)}


@app.get("/games/today", response_model=list[GameState])
async def get_today_games():
    """
    Return all of today's games from the NHL API, merged with live DB data.

    Games not yet tracked in the DB are returned with default values so the
    frontend can display the full schedule before games start.
    """
    try:
        api_games = await fetch_schedule()
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"NHL API error: {exc}")

    # One DB query; index by game_id for O(1) lookups below
    db_rows = await database.get_today_games(_mt_today())
    db_by_id: dict[int, dict] = {row["game_id"]: row for row in db_rows}

    results = []
    for api_game in api_games:
        db_row = db_by_id.get(api_game["game_id"])
        if db_row:
            state = _row_to_game_state(db_row)
            state = state.model_copy(update={"game_state": api_game["game_state"], "start_time_utc": api_game.get("start_time_utc")})
        else:
            state = _default_game_state(api_game)
        results.append(state)

    return results


@app.get("/games/date/{date}", response_model=list[GameState])
async def get_games_by_date(date: str):
    """
    Return games for a specific MT date (YYYY-MM-DD).

    - Past dates: DB records only (game_date column).
    - Today (MT): NHL API merged with DB, same as /games/today.
    - Future dates: NHL API schedule with default values.
    """
    try:
        target_date = PyDate.fromisoformat(date)
    except ValueError:
        raise HTTPException(status_code=400, detail="Date must be YYYY-MM-DD")

    today_mt = _mt_today()

    if target_date == today_mt:
        # Delegate to the same logic as /games/today
        try:
            api_games = await fetch_schedule()
        except Exception as exc:
            raise HTTPException(status_code=502, detail=f"NHL API error: {exc}")
        db_rows = await database.get_today_games(today_mt)
        db_by_id = {row["game_id"]: row for row in db_rows}
        results = []
        for api_game in api_games:
            db_row = db_by_id.get(api_game["game_id"])
            if db_row:
                state = _row_to_game_state(db_row)
                state = state.model_copy(update={"game_state": api_game["game_state"], "start_time_utc": api_game.get("start_time_utc")})
            else:
                state = _default_game_state(api_game)
            results.append(state)
        return results

    if target_date < today_mt:
        # Past date: schedule → boxscore (concurrent) → merge with DB records
        try:
            api_games = await fetch_schedule(date)
        except Exception as exc:
            raise HTTPException(status_code=502, detail=f"NHL API error: {exc}")

        db_rows = await database.get_games_by_date(target_date)
        db_by_id = {row["game_id"]: row for row in db_rows}

        async with httpx.AsyncClient(timeout=10.0) as client:
            boxscores = await asyncio.gather(
                *[_fetch_boxscore(client, g["game_id"]) for g in api_games]
            )

        results = []
        for api_game, boxscore in zip(api_games, boxscores):
            db_row = db_by_id.get(api_game["game_id"])
            if boxscore:
                results.append(_boxscore_to_game_state(api_game, boxscore, db_row))
            elif db_row:
                results.append(_row_to_game_state(db_row))
            else:
                results.append(_default_game_state(api_game))
        return results

    # Future date: NHL API only, no live data
    try:
        api_games = await fetch_schedule(date)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"NHL API error: {exc}")
    return [_default_game_state(g) for g in api_games]


def _mmss_to_seconds(t: str) -> int:
    """Parse a MM:SS clock string to total seconds. Returns 0 on bad input."""
    try:
        m, s = t.split(":")
        return int(m) * 60 + int(s)
    except (ValueError, AttributeError):
        return 0


def _period_duration(period: int) -> int:
    """Return period length in seconds (OT = 5 min, regulation = 20 min)."""
    return 300 if period > 3 else 1200


@app.get("/games/{game_id}/events", response_model=list[TiltEvent])
async def get_game_events(game_id: int):
    """
    Return active tilt events for a game, filtered to the last 3 minutes
    of game clock within the current period.
    """
    rows, game = await asyncio.gather(
        database.get_active_events(game_id),
        database.get_game(game_id),
    )
    if not rows:
        raise HTTPException(status_code=404, detail=f"No active events for game {game_id}")

    time_remaining = game["time_remaining"] if game else ""

    if game and time_remaining and time_remaining != "Intermission":
        current_period = game["period"]
        elapsed = _period_duration(current_period) - _mmss_to_seconds(time_remaining)
        cutoff = max(0, elapsed - 180)  # events from last 3 minutes

        rows = [
            r for r in rows
            if r["period"] == current_period
            and _mmss_to_seconds(r["time_in_period"]) >= cutoff
        ]
    elif time_remaining == "Intermission":
        rows = []

    return [TiltEvent(**r) for r in rows]


@app.get("/games/{game_id}", response_model=GameState)
async def get_game(game_id: int):
    """Return the current state for a single game."""
    row = await database.get_game(game_id)
    if not row:
        raise HTTPException(status_code=404, detail=f"Game {game_id} not found")
    return _row_to_game_state(row)


@app.get("/games/{game_id}/tilt", response_model=TiltResponse)
async def get_tilt(
    game_id: int,
    full: bool = Query(default=False, description="Return all records ascending instead of last 20"),
):
    """Return current tilt scores and tilt history. Use ?full=true for complete game history."""
    history_rows = await database.get_tilt_history(game_id, full=full)
    if not history_rows:
        raise HTTPException(status_code=404, detail=f"No tilt data for game {game_id}")

    # full=true returns ASC (oldest first); full=false returns DESC (newest first)
    latest = history_rows[-1] if full else history_rows[0]
    return TiltResponse(
        game_id=game_id,
        net_tilt=latest["net_tilt"],
        home_score=latest["home_score"],
        away_score=latest["away_score"],
        history=[
            TiltRecord(
                id=r["id"],
                game_id=r["game_id"],
                net_tilt=r["net_tilt"],
                home_score=r["home_score"],
                away_score=r["away_score"],
                period=r["period"],
                time_remaining=r["time_remaining"],
                timestamp=r["timestamp"],
            )
            for r in history_rows
        ],
    )
