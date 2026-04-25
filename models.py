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
    win_probability: Optional[float] = None
    updated_at: datetime


class TiltRecord(BaseModel):
    id: Optional[int] = None
    game_id: int
    net_tilt: float
    home_score: float
    away_score: float
    timestamp: datetime


class TiltResponse(BaseModel):
    game_id: int
    net_tilt: float
    home_score: float
    away_score: float
    history: list[TiltRecord]


class HealthResponse(BaseModel):
    status: str
