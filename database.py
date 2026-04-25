"""
Postgres connection pool and query helpers.

Uses asyncpg directly for async performance. Call init_db() on startup
to create tables if they don't exist, then use the module-level pool
for all queries.
"""

import asyncpg
import os
from datetime import datetime, timezone
from typing import Optional

# Module-level pool — initialized once at startup via init_db()
_pool: Optional[asyncpg.Pool] = None


async def init_db() -> None:
    """Create the connection pool and ensure tables exist."""
    global _pool
    database_url = os.environ["DATABASE_URL"]
    _pool = await asyncpg.create_pool(database_url, min_size=2, max_size=10)
    await _create_tables()


async def close_db() -> None:
    if _pool:
        await _pool.close()


def get_pool() -> asyncpg.Pool:
    if _pool is None:
        raise RuntimeError("Database pool is not initialized. Call init_db() first.")
    return _pool


async def _create_tables() -> None:
    async with get_pool().acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS games (
                game_id         BIGINT PRIMARY KEY,
                home_team       TEXT NOT NULL,
                away_team       TEXT NOT NULL,
                home_score      INTEGER NOT NULL DEFAULT 0,
                away_score      INTEGER NOT NULL DEFAULT 0,
                period          INTEGER NOT NULL DEFAULT 0,
                time_remaining  TEXT NOT NULL DEFAULT '',
                game_state      TEXT NOT NULL DEFAULT 'PRE',
                strength        TEXT NOT NULL DEFAULT 'evenStrength',
                empty_net       TEXT NOT NULL DEFAULT 'none',
                win_probability DOUBLE PRECISION,
                updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS tilt_history (
                id             BIGSERIAL PRIMARY KEY,
                game_id        BIGINT NOT NULL REFERENCES games(game_id) ON DELETE CASCADE,
                net_tilt       DOUBLE PRECISION NOT NULL,
                home_score     DOUBLE PRECISION NOT NULL,
                away_score     DOUBLE PRECISION NOT NULL,
                period         INTEGER NOT NULL DEFAULT 0,
                time_remaining TEXT NOT NULL DEFAULT '',
                timestamp      TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)

        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_tilt_history_game_id
            ON tilt_history (game_id, timestamp DESC)
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS tilt_events (
                id             BIGSERIAL PRIMARY KEY,
                game_id        BIGINT NOT NULL REFERENCES games(game_id) ON DELETE CASCADE,
                event_id       BIGINT NOT NULL,
                sort_order     INTEGER NOT NULL,
                event_type     TEXT NOT NULL,
                team_abbrev    TEXT NOT NULL,
                base_weight    DOUBLE PRECISION NOT NULL,
                decayed_weight DOUBLE PRECISION NOT NULL,
                time_in_period TEXT NOT NULL,
                period         INTEGER NOT NULL,
                created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE (game_id, sort_order)
            )
        """)

        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_tilt_events_game_id
            ON tilt_events (game_id, sort_order ASC)
        """)


# ---------------------------------------------------------------------------
# Game queries
# ---------------------------------------------------------------------------

async def upsert_game(
    game_id: int,
    home_team: str,
    away_team: str,
    home_score: int,
    away_score: int,
    period: int,
    time_remaining: str,
    game_state: str,
    strength: str,
    empty_net: str,
    win_probability: Optional[float] = None,
) -> None:
    async with get_pool().acquire() as conn:
        await conn.execute(
            """
            INSERT INTO games (
                game_id, home_team, away_team, home_score, away_score,
                period, time_remaining, game_state, strength, empty_net,
                win_probability, updated_at
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
            ON CONFLICT (game_id) DO UPDATE SET
                home_score      = EXCLUDED.home_score,
                away_score      = EXCLUDED.away_score,
                period          = EXCLUDED.period,
                time_remaining  = EXCLUDED.time_remaining,
                game_state      = EXCLUDED.game_state,
                strength        = EXCLUDED.strength,
                empty_net       = EXCLUDED.empty_net,
                win_probability = EXCLUDED.win_probability,
                updated_at      = EXCLUDED.updated_at
            """,
            game_id, home_team, away_team, home_score, away_score,
            period, time_remaining, game_state, strength, empty_net,
            win_probability, datetime.now(timezone.utc),
        )


async def get_game(game_id: int) -> Optional[dict]:
    async with get_pool().acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM games WHERE game_id = $1", game_id
        )
    return dict(row) if row else None


async def get_today_games() -> list[dict]:
    """Return all games whose updated_at is within the current UTC day."""
    async with get_pool().acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT * FROM games
            WHERE updated_at >= CURRENT_DATE
            ORDER BY updated_at DESC
            """
        )
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Tilt history queries
# ---------------------------------------------------------------------------

async def insert_tilt(
    game_id: int,
    net_tilt: float,
    home_score: float,
    away_score: float,
    period: int,
    time_remaining: str,
) -> None:
    async with get_pool().acquire() as conn:
        await conn.execute(
            """
            INSERT INTO tilt_history (
                game_id, net_tilt, home_score, away_score,
                period, time_remaining, timestamp
            ) VALUES ($1, $2, $3, $4, $5, $6, $7)
            """,
            game_id, net_tilt, home_score, away_score,
            period, time_remaining, datetime.now(timezone.utc),
        )


async def get_tilt_history(game_id: int, limit: int = 20) -> list[dict]:
    async with get_pool().acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT * FROM tilt_history
            WHERE game_id = $1
            ORDER BY timestamp DESC
            LIMIT $2
            """,
            game_id, limit,
        )
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Tilt events queries (rolling window snapshot)
# ---------------------------------------------------------------------------

async def upsert_tilt_events(game_id: int, events: list[dict]) -> None:
    """
    Replace the stored rolling-window snapshot for a game.

    Upserts all currently active events by (game_id, sort_order), then
    deletes any rows for this game that are no longer in the active window.
    """
    async with get_pool().acquire() as conn:
        if not events:
            await conn.execute(
                "DELETE FROM tilt_events WHERE game_id = $1", game_id
            )
            return

        await conn.executemany(
            """
            INSERT INTO tilt_events (
                game_id, event_id, sort_order, event_type, team_abbrev,
                base_weight, decayed_weight, time_in_period, period, created_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW())
            ON CONFLICT (game_id, sort_order) DO UPDATE SET
                decayed_weight = EXCLUDED.decayed_weight
            """,
            [
                (
                    game_id,
                    e["event_id"],
                    e["sort_order"],
                    e["event_type"],
                    e["team_abbrev"],
                    e["base_weight"],
                    e["decayed_weight"],
                    e["time_in_period"],
                    e["period"],
                )
                for e in events
            ],
        )

        # Remove events that have aged out of the window
        active_sort_orders = [e["sort_order"] for e in events]
        await conn.execute(
            "DELETE FROM tilt_events WHERE game_id = $1 AND sort_order != ALL($2)",
            game_id, active_sort_orders,
        )


async def get_active_events(game_id: int) -> list[dict]:
    """Return the current rolling-window events for a game, oldest first."""
    async with get_pool().acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT * FROM tilt_events
            WHERE game_id = $1
            ORDER BY sort_order ASC
            """,
            game_id,
        )
    return [dict(r) for r in rows]
