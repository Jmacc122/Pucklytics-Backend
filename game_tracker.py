"""
Fast mode — live game tracker.

One coroutine per active game, polling the NHL play-by-play API every 3 seconds.
Calls tilt.py after each poll, writes state to Postgres, then shuts itself down
10 minutes after the game ends.
"""

import asyncio
import logging
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import httpx

import database
from tilt import TiltEngine, mmss_to_seconds

MT = ZoneInfo("America/Denver")

logger = logging.getLogger(__name__)

POLL_INTERVAL = 3       # seconds between polls during live game
SHUTDOWN_DELAY = 600    # seconds to wait after game ends before exiting
TERMINAL_STATES = {"OFF", "FINAL"}

NHL_PBP_URL = "https://api-web.nhle.com/v1/gamecenter/{game_id}/play-by-play"


def _current_game_seconds(period: int, time_remaining: str) -> int:
    """
    Convert current period + time_remaining (countdown) to total game seconds elapsed.
    Periods 1-3 are 1200s each; OT periods (4+) are 300s each.
    """
    if period < 1 or not time_remaining:
        return 0
    period_dur = 300 if period > 3 else 1200
    elapsed = period_dur - mmss_to_seconds(time_remaining)
    if period <= 3:
        return (period - 1) * 1200 + elapsed
    return 3 * 1200 + (period - 4) * 300 + elapsed


def _parse_situation_code(code: str) -> str:
    """
    Decode the 4-digit situationCode into an empty-net label.

    NHL situationCode layout (left → right): away-goalie, away-skaters,
    home-skaters, home-goalie.  A digit of 0 in the goalie position means
    the net is empty.

      code[0] == '0'  →  away goalie pulled  (away empty net)
      code[3] == '0'  →  home goalie pulled  (home empty net)
    """
    if len(code) < 4:
        return "none"
    away_empty = code[0] == "0"
    home_empty = code[3] == "0"
    if away_empty and home_empty:
        return "both"
    if away_empty:
        return "away"
    if home_empty:
        return "home"
    return "none"


def _parse_strength(code: str) -> str:
    """
    Derive strength state from situationCode (digits 1 and 2, 0-indexed).

    away-skaters (code[1]) vs home-skaters (code[2]).
    Equal → even strength.  Unequal → power play for the side with more skaters.
    """
    if len(code) < 4:
        return "evenStrength"
    away_skaters = int(code[1])
    home_skaters = int(code[2])
    if away_skaters == home_skaters:
        return "evenStrength"
    return "powerPlay"


async def track_game(game_id: int) -> None:
    """
    Poll the NHL play-by-play endpoint for `game_id` until the game ends,
    then wait SHUTDOWN_DELAY seconds before returning.
    """
    logger.info("Fast mode starting for game %s", game_id)
    engine = TiltEngine(game_id)
    seen_event_ids: set[int] = set()
    terminal_seen_at: datetime | None = None
    last_game_seconds: int = 0       # frozen during intermissions
    prev_strength: str = "evenStrength"
    game_date_mt = datetime.now(MT).date()  # MT date when tracking started
    en_goals: int = 0                # cumulative EN goals this game

    async with httpx.AsyncClient(timeout=10.0) as client:
        while True:
            try:
                resp = await client.get(NHL_PBP_URL.format(game_id=game_id))
                resp.raise_for_status()
                data = resp.json()
            except Exception as exc:
                logger.warning("Poll error for game %s: %s", game_id, exc)
                await asyncio.sleep(POLL_INTERVAL)
                continue

            # ----------------------------------------------------------------
            # Parse game-level metadata
            # ----------------------------------------------------------------
            game_state = data.get("gameState", "UNKNOWN")
            start_time_utc = data.get("startTimeUTC", "")
            situation_code = data.get("situation", {}).get("situationCode", "1551")

            home_info = data.get("homeTeam", {})
            away_info = data.get("awayTeam", {})
            home_team = home_info.get("abbrev", "HOME")
            away_team = away_info.get("abbrev", "AWAY")
            home_score = home_info.get("score", 0)
            away_score = away_info.get("score", 0)
            home_sog = home_info.get("sog", 0)
            away_sog = away_info.get("sog", 0)

            period_desc = data.get("periodDescriptor", {})
            period = period_desc.get("number", 0)

            # ----------------------------------------------------------------
            # Intermission — freeze the game clock; don't advance decay
            # ----------------------------------------------------------------
            clock = data.get("clock", {})
            if clock.get("inIntermission", False):
                time_remaining = "Intermission"
                strength = "evenStrength"
                # last_game_seconds intentionally not updated here
            else:
                time_remaining = clock.get("timeRemaining", "")
                strength = _parse_strength(situation_code)
                if period > 0 and time_remaining:
                    last_game_seconds = _current_game_seconds(period, time_remaining)

            empty_net = _parse_situation_code(situation_code)

            # ----------------------------------------------------------------
            # Flush penalty events when power play ends
            # ----------------------------------------------------------------
            if prev_strength == "powerPlay" and strength == "evenStrength":
                engine.flush_penalties()
                logger.debug("Game %s | power play ended — penalties flushed", game_id)
            prev_strength = strength

            # ----------------------------------------------------------------
            # Feed new play events into the tilt engine; count EN goals
            # ----------------------------------------------------------------
            for event in data.get("plays", []):
                event_id = event.get("eventId")
                if event_id in seen_event_ids:
                    continue
                seen_event_ids.add(event_id)
                engine.push_event(event, home_info, away_info)

                if event.get("typeDescKey") == "goal":
                    # Prefer the situationCode embedded in the event; fall back
                    # to the game-level code polled at this moment
                    code = event.get("situationCode", situation_code)
                    if len(code) >= 4 and (code[0] == "0" or code[3] == "0"):
                        en_goals += 1

            net_tilt, tilt_home, tilt_away = engine.calculate(last_game_seconds)
            active_events = engine.get_active_events(last_game_seconds)

            # ----------------------------------------------------------------
            # Persist game state, tilt snapshot, and rolling-window events
            # ----------------------------------------------------------------
            try:
                await database.upsert_game(
                    game_id=game_id,
                    home_team=home_team,
                    away_team=away_team,
                    home_score=home_score,
                    away_score=away_score,
                    period=period,
                    time_remaining=time_remaining,
                    game_state=game_state,
                    strength=strength,
                    empty_net=empty_net,
                    home_sog=home_sog,
                    away_sog=away_sog,
                    en_goals=en_goals,
                    start_time_utc=start_time_utc,
                    game_date=game_date_mt,
                )
                await database.insert_tilt(
                    game_id=game_id,
                    net_tilt=net_tilt,
                    home_score=tilt_home,
                    away_score=tilt_away,
                    period=period,
                    time_remaining=time_remaining,
                )
                await database.upsert_tilt_events(game_id, active_events)
            except Exception as exc:
                logger.error("DB write error for game %s: %s", game_id, exc)

            logger.debug(
                "Game %s | state=%s period=%s %s | tilt=%.3f | events=%d",
                game_id, game_state, period, time_remaining, net_tilt, len(active_events),
            )

            # ----------------------------------------------------------------
            # Shutdown logic — wait 10 minutes after game ends
            # ----------------------------------------------------------------
            if game_state in TERMINAL_STATES:
                if terminal_seen_at is None:
                    terminal_seen_at = datetime.now(timezone.utc)
                    logger.info(
                        "Game %s finished (%s). Shutting down in %ds.",
                        game_id, game_state, SHUTDOWN_DELAY,
                    )
                elapsed = (datetime.now(timezone.utc) - terminal_seen_at).total_seconds()
                if elapsed >= SHUTDOWN_DELAY:
                    logger.info("Fast mode exiting for game %s", game_id)
                    return

            await asyncio.sleep(POLL_INTERVAL)
