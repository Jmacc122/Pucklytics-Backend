"""
Pucklytics — FastAPI entry point.

Starts the slow-mode scheduler on startup and exposes REST endpoints
for the frontend to consume.
"""

import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
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
        win_probability=row.get("win_probability"),
        updated_at=row["updated_at"],
    )


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

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
    Manually spin up the fast-mode tracker for the first non-final game found
    on today's schedule. For testing only — does not affect the scheduler.
    """
    try:
        games = await fetch_schedule()
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"NHL API error: {exc}")

    # Pick the first game that isn't already over
    target = next(
        (g for g in games if g["game_state"] not in {"OFF", "FINAL"}),
        None,
    )
    if target is None:
        raise HTTPException(status_code=404, detail="No active or upcoming games found")

    _ensure_fast_mode(target["game_id"])
    return {"started": True, "game_id": target["game_id"]}


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
    db_rows = await database.get_today_games()
    db_by_id: dict[int, dict] = {row["game_id"]: row for row in db_rows}

    results = []
    for api_game in api_games:
        game_id = api_game["game_id"]
        db_row = db_by_id.get(game_id)

        if db_row:
            # Merge: DB supplies live play data; API supplies the freshest game_state
            state = _row_to_game_state(db_row)
            state = state.model_copy(update={"game_state": api_game["game_state"]})
        else:
            # Game exists on the schedule but hasn't been tracked yet
            state = GameState(
                game_id=game_id,
                home_team=api_game["home_team"],
                away_team=api_game["away_team"],
                home_score=0,
                away_score=0,
                period=0,
                time_remaining="",
                game_state=api_game["game_state"],
                strength="evenStrength",
                empty_net="none",
                win_probability=None,
                updated_at=datetime.now(timezone.utc),
            )

        results.append(state)

    return results


@app.get("/games/{game_id}/events", response_model=list[TiltEvent])
async def get_game_events(game_id: int):
    """Return the current active events in the tilt rolling window for a game."""
    rows = await database.get_active_events(game_id)
    if not rows:
        raise HTTPException(status_code=404, detail=f"No active events for game {game_id}")
    return [TiltEvent(**r) for r in rows]


@app.get("/games/{game_id}", response_model=GameState)
async def get_game(game_id: int):
    """Return the current state for a single game."""
    row = await database.get_game(game_id)
    if not row:
        raise HTTPException(status_code=404, detail=f"Game {game_id} not found")
    return _row_to_game_state(row)


@app.get("/games/{game_id}/tilt", response_model=TiltResponse)
async def get_tilt(game_id: int):
    """Return current tilt scores and the last 20 tilt history records."""
    history_rows = await database.get_tilt_history(game_id, limit=20)
    if not history_rows:
        raise HTTPException(status_code=404, detail=f"No tilt data for game {game_id}")

    latest = history_rows[0]  # rows are ordered DESC so first is most recent
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
                timestamp=r["timestamp"],
            )
            for r in history_rows
        ],
    )
