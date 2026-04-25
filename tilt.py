"""
Ice tilt calculation engine.

Maintains a 5-minute rolling window of weighted play events per game.
Decay and expiry are based on game-clock seconds, not wall-clock time,
so stoppages and intermissions do not cause events to decay.
"""

from collections import deque
from dataclasses import dataclass, field
from typing import Deque

# Weight assigned to each NHL play event type
EVENT_WEIGHTS: dict[str, float] = {
    "shot-on-goal": 1.0,
    "missed-shot": 0.7,
    "blocked-shot": 0.4,
    "faceoff": 0.1,
    "takeaway": 0.1,
    "penalty": 2.0,
    "goal": 2.0,
}

# Step decay by game-clock age bucket (seconds)
# Bucket boundaries: [0,60), [60,120), [120,180), [180,240), [240,300), [300,∞)
DECAY_STEPS = [
    (60,  1.00),
    (120, 0.80),
    (180, 0.60),
    (240, 0.40),
    (300, 0.20),
]

WINDOW_SECONDS = 300  # 5 game minutes


def mmss_to_seconds(t: str) -> int:
    """Parse a MM:SS string to total seconds. Returns 0 on bad input."""
    try:
        m, s = t.split(":")
        return int(m) * 60 + int(s)
    except (ValueError, AttributeError):
        return 0


def game_seconds_from_elapsed(period: int, time_in_period: str) -> int:
    """
    Convert period + timeInPeriod (elapsed) to total game seconds.
    Periods 1-3 are 1200s each; OT periods (4+) are 300s each.
    """
    if period < 1:
        return 0
    elapsed = mmss_to_seconds(time_in_period)
    if period <= 3:
        return (period - 1) * 1200 + elapsed
    return 3 * 1200 + (period - 4) * 300 + elapsed


@dataclass
class WeightedEvent:
    event_id: int               # NHL API eventId
    sort_order: int             # NHL API sortOrder — upsert key in DB
    team: str                   # "home" or "away"
    team_abbrev: str            # e.g. "TOR"
    event_type: str
    base_weight: float
    game_seconds_elapsed: int   # total game-clock seconds when event occurred
    period: int
    time_in_period: str         # MM:SS elapsed in period, e.g. "12:34"
    penalty_duration: int = field(default=0)  # game seconds; 0 for non-penalties


def _decay_factor(age_seconds: float) -> float:
    """Return the step-decay multiplier for a given game-clock age."""
    for threshold, factor in DECAY_STEPS:
        if age_seconds < threshold:
            return factor
    return 0.0


class TiltEngine:
    """Per-game tilt calculator. One instance per active game."""

    def __init__(self, game_id: int):
        self.game_id = game_id
        self._queue: Deque[WeightedEvent] = deque()
        self._current_period: int = 0

    def push_event(self, event: dict, home_team: dict, away_team: dict) -> None:
        """
        Ingest a raw play-by-play event dict from the NHL API.

        home_team / away_team are the full team objects (abbrev + id).
        Team resolution: prefer eventOwnerTeamAbbrev, fall back to team ID match.
        Penalty weight is assigned to the non-offending (power-play) team.
        """
        event_type = event.get("typeDescKey", "")
        weight = EVENT_WEIGHTS.get(event_type)
        if weight is None:
            return

        period = event.get("periodDescriptor", {}).get("number", self._current_period)

        # Hard reset on period change — flush the rolling window
        if period != self._current_period and self._current_period != 0:
            self._queue.clear()
        self._current_period = period

        home_abbrev = home_team.get("abbrev", "")
        away_abbrev = away_team.get("abbrev", "")
        details = event.get("details", {})

        team_abbrev = details.get("eventOwnerTeamAbbrev", "")
        if not team_abbrev:
            event_team_id = details.get("eventOwnerTeamId")
            if event_team_id == home_team.get("id"):
                team_abbrev = home_abbrev
            elif event_team_id == away_team.get("id"):
                team_abbrev = away_abbrev

        team_side = "home" if team_abbrev == home_abbrev else "away"

        # Penalty: flip to the power-play team and record duration
        penalty_duration = 0
        if event_type == "penalty":
            team_side = "away" if team_side == "home" else "home"
            team_abbrev = away_abbrev if team_abbrev == home_abbrev else home_abbrev
            penalty_mins = details.get("penaltyMinutes", 2)
            penalty_duration = int(penalty_mins) * 60

        time_in_period = event.get("timeInPeriod", "")

        self._queue.append(
            WeightedEvent(
                event_id=event.get("eventId", 0),
                sort_order=event.get("sortOrder", event.get("eventId", 0)),
                team=team_side,
                team_abbrev=team_abbrev,
                event_type=event_type,
                base_weight=weight,
                game_seconds_elapsed=game_seconds_from_elapsed(period, time_in_period),
                period=period,
                time_in_period=time_in_period,
                penalty_duration=penalty_duration,
            )
        )

    def _prune_stale(self, current_game_seconds: int) -> None:
        """Remove events that have expired by game clock."""
        stale = []
        for ev in self._queue:
            age = current_game_seconds - ev.game_seconds_elapsed
            if age >= WINDOW_SECONDS:
                stale.append(ev)
            elif ev.penalty_duration > 0 and age >= ev.penalty_duration:
                # Penalty duration has elapsed — power play is over
                stale.append(ev)
        for ev in stale:
            try:
                self._queue.remove(ev)
            except ValueError:
                pass

    def flush_penalties(self) -> None:
        """
        Remove all penalty events immediately.
        Call this when strength transitions from powerPlay back to evenStrength.
        """
        self._queue = deque(ev for ev in self._queue if ev.event_type != "penalty")

    def calculate(self, current_game_seconds: int) -> tuple[float, float, float]:
        """
        Compute tilt scores using game-clock age for decay.

        Returns (net_tilt, home_score, away_score).
        net_tilt > 0 means home team has tilt advantage.
        """
        self._prune_stale(current_game_seconds)

        home_score = 0.0
        away_score = 0.0
        for ev in self._queue:
            age = current_game_seconds - ev.game_seconds_elapsed
            weighted = ev.base_weight * _decay_factor(age)
            if ev.team == "home":
                home_score += weighted
            else:
                away_score += weighted

        net_tilt = round(home_score - away_score, 4)
        return net_tilt, round(home_score, 4), round(away_score, 4)

    def get_active_events(self, current_game_seconds: int) -> list[dict]:
        """
        Return all active events in the rolling window with current decayed weights.
        Call after calculate() so stale events are already pruned.
        """
        result = []
        for ev in self._queue:
            age = current_game_seconds - ev.game_seconds_elapsed
            if age >= WINDOW_SECONDS:
                continue
            if ev.penalty_duration > 0 and age >= ev.penalty_duration:
                continue
            result.append({
                "event_id": ev.event_id,
                "sort_order": ev.sort_order,
                "event_type": ev.event_type,
                "team_abbrev": ev.team_abbrev,
                "base_weight": ev.base_weight,
                "decayed_weight": round(ev.base_weight * _decay_factor(age), 4),
                "time_in_period": ev.time_in_period,
                "period": ev.period,
            })
        return result
