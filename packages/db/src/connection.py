"""
Neon Database Connection

Provides sync and async connections to Neon PostgreSQL.
"""

import os
from typing import Optional
from contextlib import contextmanager, asynccontextmanager

import psycopg
from psycopg.rows import dict_row
import asyncpg


def get_database_url() -> str:
    """Get database URL from environment."""
    url = os.getenv("DATABASE_URL")
    if not url:
        raise ValueError("DATABASE_URL not set")
    return url


@contextmanager
def get_connection():
    """
    Get a sync database connection.

    Usage:
        with get_connection() as conn:
            result = conn.execute("SELECT 1").fetchone()
    """
    conn = psycopg.connect(get_database_url(), row_factory=dict_row)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


@asynccontextmanager
async def get_async_connection():
    """
    Get an async database connection.

    Usage:
        async with get_async_connection() as conn:
            result = await conn.fetchrow("SELECT 1")
    """
    conn = await asyncpg.connect(get_database_url())
    try:
        yield conn
    finally:
        await conn.close()


class DatabasePool:
    """
    Connection pool for high-throughput scenarios.
    Use in FastAPI/workers for connection reuse.
    """

    _pool: Optional[asyncpg.Pool] = None

    @classmethod
    async def get_pool(cls) -> asyncpg.Pool:
        if cls._pool is None:
            cls._pool = await asyncpg.create_pool(
                get_database_url(),
                min_size=2,
                max_size=10,
            )
        return cls._pool

    @classmethod
    async def close(cls):
        if cls._pool:
            await cls._pool.close()
            cls._pool = None

    @classmethod
    @asynccontextmanager
    async def acquire(cls):
        """Acquire a connection from the pool."""
        pool = await cls.get_pool()
        async with pool.acquire() as conn:
            yield conn
