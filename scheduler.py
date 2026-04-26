"""
Slow mode — hourly scheduler.

Polls the NHL schedule endpoint once per hour. If any game starts within
the next 60 minutes, it launches a fast-mode coroutine for that game.
Each game gets its own asyncio task so multiple games run concurrently.
"""

import asyncio
import logging
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

import httpx
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from game_tracker import track_game

logger = logging.getLogger(__name__)

NHL_SCHEDULE_BASE_URL = "https://api-web.nhle.com/v1/schedule"
CHECK_INTERVAL_MINUTES = 60
LOOKAHEAD_SECONDS = CHECK_INTERVAL_MINUTES * 60  # start fast mode up to 60 min early

MT = ZoneInfo("America/Denver")

# Tracks game_id → asyncio.Task to avoid spawning duplicates
_active_tasks: dict[int, asyncio.Task] = {}


async def fetch_schedule(date_str: str | None = None) -> list[dict]:
    """
    Hit the NHL schedule endpoint and return a parsed list of games.

    date_str: YYYY-MM-DD in Mountain Time. Defaults to today in MT.
    Each dict contains: game_id, home_team, away_team, start_time_utc,
    game_state, starts_soon (bool), is_live (bool).
    Raises httpx.HTTPError on network failure.
    """
    if date_str is None:
        date_str = datetime.now(MT).strftime("%Y-%m-%d")
    url = f"{NHL_SCHEDULE_BASE_URL}/{date_str}"
    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.get(url)
        resp.raise_for_status()
        data = resp.json()

    now = datetime.now(MT)
    upcoming_cutoff = now + timedelta(seconds=LOOKAHEAD_SECONDS)

    game_weeks = data.get("gameWeek", [])
    if not game_weeks:
        return []

    results = []
    for game in game_weeks[0].get("games", []):
        game_id: int = game.get("id")
        start_time_str: str = game.get("startTimeUTC", "")
        game_state: str = game.get("gameState", "")

        if not game_id or not start_time_str:
            continue

        try:
            start_time = datetime.fromisoformat(start_time_str.replace("Z", "+00:00"))
        except ValueError:
            start_time = None

        is_live = game_state in {"LIVE", "CRIT"}
        starts_soon = (
            start_time is not None and now <= start_time <= upcoming_cutoff
        )

        results.append({
            "game_id": game_id,
            "home_team": game.get("homeTeam", {}).get("abbrev", ""),
            "away_team": game.get("awayTeam", {}).get("abbrev", ""),
            "start_time_utc": start_time_str,
            "game_state": game_state,
            "is_live": is_live,
            "starts_soon": starts_soon,
        })

    return results


async def check_schedule() -> None:
    """
    Fetch today's schedule and spin up fast mode for any game starting soon.
    Called by APScheduler every hour.
    """
    logger.info("Scheduler: checking NHL schedule")
    try:
        games = await fetch_schedule()
    except Exception as exc:
        logger.error("Scheduler: failed to fetch schedule — %s", exc)
        return

    logger.info("Scheduler: found %d game(s) today", len(games))

    for game in games:
        if game["game_state"] in {"OFF", "FINAL"}:
            continue
        if game["is_live"] or game["starts_soon"]:
            _ensure_fast_mode(game["game_id"])


def _ensure_fast_mode(game_id: int) -> None:
    """Launch a fast-mode task for `game_id` if one isn't already running."""
    existing = _active_tasks.get(game_id)
    if existing and not existing.done():
        logger.debug("Scheduler: fast mode already running for game %s", game_id)
        return

    logger.info("Scheduler: spinning up fast mode for game %s", game_id)
    task = asyncio.create_task(track_game(game_id), name=f"fast-mode-{game_id}")
    _active_tasks[game_id] = task

    # Log when a task finishes so we know it exited cleanly (or with an error)
    task.add_done_callback(lambda t: _on_task_done(game_id, t))


def _on_task_done(game_id: int, task: asyncio.Task) -> None:
    if task.exception():
        logger.error("Fast mode for game %s raised: %s", game_id, task.exception())
    else:
        logger.info("Fast mode for game %s completed normally", game_id)
    _active_tasks.pop(game_id, None)


def create_scheduler() -> AsyncIOScheduler:
    """Build and return a configured APScheduler instance (not yet started)."""
    scheduler = AsyncIOScheduler()
    scheduler.add_job(
        check_schedule,
        trigger="interval",
        minutes=CHECK_INTERVAL_MINUTES,
        id="nhl_schedule_check",
        name="NHL schedule check (slow mode)",
        next_run_time=datetime.now(timezone.utc),  # run immediately on startup
    )
    return scheduler
