"""
Ice tilt calculation engine.

Maintains a 5-minute rolling window of weighted play events per game.
Returns net_tilt (positive = home advantage, negative = away advantage).
"""

from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
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

# Step decay factors keyed by age bucket (seconds elapsed since event)
# Bucket boundaries: [0,60), [60,120), [120,180), [180,240), [240,300), [300,∞)
DECAY_STEPS = [
    (60,  1.00),
    (120, 0.80),
    (180, 0.60),
    (240, 0.40),
    (300, 0.20),
]

WINDOW_SECONDS = 300  # 5 minutes


@dataclass
class WeightedEvent:
    event_id: int          # NHL API eventId
    sort_order: int        # NHL API sortOrder — used as upsert key in DB
    team: str              # "home" or "away"
    team_abbrev: str       # actual team abbreviation, e.g. "TOR"
    event_type: str
    base_weight: float
    occurred_at: datetime
    period: int
    time_in_period: str    # e.g. "12:34"


def _decay_factor(age_seconds: float) -> float:
    """Return the step-decay multiplier for an event that is `age_seconds` old."""
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

        home_team / away_team are the raw team objects from the API response,
        each containing at minimum 'abbrev' and 'id'.

        Team resolution: prefer eventOwnerTeamAbbrev from event details;
        fall back to matching eventOwnerTeamId against home/away IDs.
        """
        event_type = event.get("typeDescKey", "")
        weight = EVENT_WEIGHTS.get(event_type)
        if weight is None:
            return  # Ignore event types we don't track

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
            # Fallback: resolve by numeric team ID
            event_team_id = details.get("eventOwnerTeamId")
            if event_team_id == home_team.get("id"):
                team_abbrev = home_abbrev
            elif event_team_id == away_team.get("id"):
                team_abbrev = away_abbrev

        team_side = "home" if team_abbrev == home_abbrev else "away"

        self._queue.append(
            WeightedEvent(
                event_id=event.get("eventId", 0),
                sort_order=event.get("sortOrder", event.get("eventId", 0)),
                team=team_side,
                team_abbrev=team_abbrev,
                event_type=event_type,
                base_weight=weight,
                occurred_at=datetime.now(timezone.utc),
                period=period,
                time_in_period=event.get("timeInPeriod", ""),
            )
        )

    def _prune_stale(self, now: datetime) -> None:
        """Remove events that have aged past the 5-minute window."""
        stale = [ev for ev in self._queue
                 if (now - ev.occurred_at).total_seconds() >= WINDOW_SECONDS]
        for ev in stale:
            try:
                self._queue.remove(ev)
            except ValueError:
                pass

    def calculate(self) -> tuple[float, float, float]:
        """
        Compute current tilt scores from the rolling window.

        Returns:
            (net_tilt, home_score, away_score)
            net_tilt > 0 means home team has tilt advantage.
        """
        now = datetime.now(timezone.utc)
        self._prune_stale(now)

        home_score = 0.0
        away_score = 0.0
        for ev in self._queue:
            age = (now - ev.occurred_at).total_seconds()
            weighted = ev.base_weight * _decay_factor(age)
            if ev.team == "home":
                home_score += weighted
            else:
                away_score += weighted

        net_tilt = round(home_score - away_score, 4)
        return net_tilt, round(home_score, 4), round(away_score, 4)

    def get_active_events(self) -> list[dict]:
        """
        Return all events currently in the rolling window with their
        current decayed weights. Call after calculate() so stale events
        are already pruned.
        """
        now = datetime.now(timezone.utc)
        result = []
        for ev in self._queue:
            age = (now - ev.occurred_at).total_seconds()
            if age >= WINDOW_SECONDS:
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
