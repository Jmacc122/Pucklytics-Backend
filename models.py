from pydantic import BaseModel
from typing import Optional
from datetime import datetime


class GameState(BaseModel):
    game_id: int
    home_team: str
    away_team: str
    home_score: int
    away_score: int
    period: int
    time_remaining: str
    game_state: str  # "LIVE", "PRE", "FINAL", "OFF", etc.
    strength: str    # "evenStrength", "powerPlay", "penaltyShot"
    empty_net: str   # "none", "home", "away", "both"
    home_sog: int = 0
    away_sog: int = 0
    en_goals: int = 0
    start_time_utc: Optional[str] = None
    win_probability: Optional[float] = None
    updated_at: datetime


class TiltRecord(BaseModel):
    id: Optional[int] = None
    game_id: int
    net_tilt: float
    home_score: float
    away_score: float
    period: int
    time_remaining: str
    timestamp: datetime


class TiltResponse(BaseModel):
    game_id: int
    net_tilt: float
    home_score: float
    away_score: float
    history: list[TiltRecord]


class TiltEvent(BaseModel):
    id: Optional[int] = None
    game_id: int
    event_id: int
    sort_order: int
    event_type: str
    team_abbrev: str
    base_weight: float
    decayed_weight: float
    time_in_period: str
    period: int
    created_at: datetime


class HealthResponse(BaseModel):
    status: str
