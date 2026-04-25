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
    team: str          # "home" or "away"
    event_type: str
    base_weight: float
    occurred_at: datetime
    period: int


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

    def push_event(self, event: dict, home_team_abbrev: str) -> None:
        """
        Ingest a raw play-by-play event dict from the NHL API.

        Expected fields: typeDescKey, eventOwnerTeamId / team abbrev,
        periodDescriptor.number, timeInPeriod.
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

        # Resolve which side owns the event
        event_team = event.get("details", {}).get("eventOwnerTeamAbbrev", "")
        team_side = "home" if event_team == home_team_abbrev else "away"

        self._queue.append(
            WeightedEvent(
                team=team_side,
                event_type=event_type,
                base_weight=weight,
                occurred_at=datetime.now(timezone.utc),
                period=period,
            )
        )

    def calculate(self) -> tuple[float, float, float]:
        """
        Compute current tilt scores from the rolling window.

        Returns:
            (net_tilt, home_score, away_score)
            net_tilt > 0 means home team has tilt advantage.
        """
        now = datetime.now(timezone.utc)
        home_score = 0.0
        away_score = 0.0
        stale: list[WeightedEvent] = []

        for ev in self._queue:
            age = (now - ev.occurred_at).total_seconds()
            if age >= WINDOW_SECONDS:
                stale.append(ev)
                continue
            weighted = ev.base_weight * _decay_factor(age)
            if ev.team == "home":
                home_score += weighted
            else:
                away_score += weighted

        # Prune expired events from the front of the deque
        for ev in stale:
            try:
                self._queue.remove(ev)
            except ValueError:
                pass

        net_tilt = round(home_score - away_score, 4)
        return net_tilt, round(home_score, 4), round(away_score, 4)
