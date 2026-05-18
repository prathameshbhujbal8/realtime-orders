"""
database.py — asyncpg connection-pool management.

A single module-level pool is shared across every request so connections
are reused efficiently rather than opened on every query.
"""

import logging
import os

import asyncpg
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# Module-level pool; populated by connect_db(), consumed everywhere else.
_pool: asyncpg.Pool | None = None


async def connect_db() -> None:
    """Create the asyncpg connection pool.  Called once at startup."""
    global _pool
    dsn = os.getenv("DATABASE_URL")
    if not dsn:
        raise RuntimeError("DATABASE_URL is not set in the environment / .env file")

    logger.info("Connecting to PostgreSQL …")
    _pool = await asyncpg.create_pool(
        dsn=dsn,
        min_size=2,        # keep at least two connections warm
        max_size=10,       # cap to avoid overwhelming Postgres
        command_timeout=60,
    )
    logger.info("PostgreSQL pool ready  (min=2, max=10)")


async def disconnect_db() -> None:
    """Gracefully close all pool connections.  Called once at shutdown."""
    global _pool
    if _pool:
        await _pool.close()
        _pool = None
        logger.info("PostgreSQL pool closed")


def get_pool() -> asyncpg.Pool:
    """
    Return the active pool.  Raises if called before connect_db().
    Use as a FastAPI dependency or call directly from service code.
    """
    if _pool is None:
        raise RuntimeError("Database pool is not initialised — call connect_db() first")
    return _pool
