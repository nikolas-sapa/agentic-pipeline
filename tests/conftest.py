"""Database tests require an explicit disposable database, never app defaults."""

import os

import pytest
from psycopg_pool import AsyncConnectionPool

from spine.db import run_migrations


@pytest.fixture
async def pool():
    database_url = os.environ.get("SPINE_TEST_DATABASE_URL")
    if not database_url:
        raise pytest.UsageError(
            "Set SPINE_TEST_DATABASE_URL to a disposable Postgres database. "
            "Tests truncate its application tables; DATABASE_URL is not used."
        )
    p = AsyncConnectionPool(conninfo=database_url, open=False)
    await p.open()
    try:
        async with p.connection() as conn:
            await run_migrations(conn)
            async with conn.cursor() as cur:
                await cur.execute(
                    "TRUNCATE events, jobs, attempts, llm_calls, alerts CASCADE"
                )
            await conn.commit()
        yield p
    finally:
        await p.close()
