"""Connection pool, transaction helper, Settings, migration runner.

No separate config.py — one Settings dataclass does not earn a file (plan §2).
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path

from psycopg import AsyncConnection
from psycopg_pool import AsyncConnectionPool

MIGRATIONS_DIR = Path(__file__).parent.parent.parent / "migrations"


@dataclass(frozen=True)
class Settings:
    database_url: str
    variant: str
    lease_seconds: int
    poll_interval_seconds: float

    @classmethod
    def from_env(cls) -> Settings:
        return cls(
            database_url=os.environ.get(
                "DATABASE_URL", "postgresql://spine:spine@localhost:5432/spine"
            ),
            variant=os.environ.get("SPINE_VARIANT", "default"),
            lease_seconds=int(os.environ.get("SPINE_LEASE_SECONDS", "30")),
            poll_interval_seconds=float(os.environ.get("SPINE_POLL_INTERVAL", "0.5")),
        )


def make_pool(settings: Settings) -> AsyncConnectionPool:
    return AsyncConnectionPool(conninfo=settings.database_url, open=False)


@asynccontextmanager
async def transaction(pool: AsyncConnectionPool):
    async with pool.connection() as conn:
        async with conn.transaction():
            yield conn


async def run_migrations(conn: AsyncConnection) -> None:
    """Apply the plain .sql migrations in order. One table set, one
    direction — Alembic is more machinery than this schema deserves (plan §5)."""
    async with conn.cursor() as cur:
        await cur.execute(
            "CREATE TABLE IF NOT EXISTS schema_migrations (filename TEXT PRIMARY KEY)"
        )
        await cur.execute("SELECT filename FROM schema_migrations")
        applied = {row[0] for row in await cur.fetchall()}

        for path in sorted(MIGRATIONS_DIR.glob("*.sql")):
            if path.name in applied:
                continue
            await cur.execute(path.read_text())
            await cur.execute(
                "INSERT INTO schema_migrations (filename) VALUES (%s)", (path.name,)
            )
    await conn.commit()
