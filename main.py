"""
Pucklytics — FastAPI entry point.

Starts the slow-mode scheduler on startup and exposes REST endpoints
for the frontend to consume.
"""

import logging
from contextlib import asynccontextmanager

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException

import database
from models import GameState, TiltResponse, TiltRecord, HealthResponse
from scheduler import create_scheduler, fetch_schedule

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


@app.get("/games/today", response_model=list[GameState])
async def get_today_games():
    """Return all games with state recorded today (UTC)."""
    rows = await database.get_today_games()
    return [_row_to_game_state(r) for r in rows]


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
