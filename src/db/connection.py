
import os
from dotenv import load_dotenv
from sqlalchemy.ext.asyncio import (AsyncEngine, AsyncSession, create_async_engine)
from sqlalchemy.orm import sessionmaker
from typing import AsyncGenerator, List, Dict, Any, Optional
import asyncpg
import logging

logger = logging.getLogger("connection")

load_dotenv()
SQLALCHEMY_URL = os.getenv("SQLALCHEMY_URL")
ASYNC_PG_URL = os.getenv("ASYNC_PG_URL")
MY_TOKEN = os.getenv("MY_TOKEN")
_pool: Optional[asyncpg.pool.Pool] = None

logger.info("[DB] Loading DB module...")


# -----------------------------------------------------------
# Create async SQLAlchemy engine (with asyncpg + pooling)
# -----------------------------------------------------------
def _create_async_engine(sqlalchemy_url: str) -> AsyncEngine:
    """
    Create an async SQLAlchemy engine using the asyncpg driver.
    """
    logger.info("[DB] Creating async engine (connection pool starts here)...")
    print
    engine: AsyncEngine = create_async_engine(
        sqlalchemy_url,
        echo=False,        # set to True for SQL logging
        future=True,
        pool_size=10,      # max number of open connections
        max_overflow=20,   # extra temporary connections
    )
    return engine


# -----------------------------------------------------------
# Create async session factory
# -----------------------------------------------------------
engine: AsyncEngine = _create_async_engine(SQLALCHEMY_URL)
AsyncSessionLocal = sessionmaker(
    engine,
    expire_on_commit=False,
    class_=AsyncSession,
)


# -----------------------------------------------------------
# Dependency / helper to get a session
# -----------------------------------------------------------
async def get_session() -> AsyncGenerator[AsyncSession, None]:
    logger.info("[DB] Opening a new async SQLAlchemy session...")

    async with AsyncSessionLocal() as session:
        logger.info("[DB] Session opened. Acquiring DB connection from pool...")
        try:
            yield session
        finally:
            logger.info("[DB] Session closing… Connection returned to pool.")


# -----------------------------------------------------------
# Shutdown helper (optional)
# -----------------------------------------------------------
async def close_engine():
    logger.info("[DB] Disposing engine... Closing all pooled connections.")
    await engine.dispose()


# ---------------------------------------------------------
# Initialize the asyncpg connection pool for streaming functions
# ---------------------------------------------------------
async def init_asyncpg_pool(
    min_size: int = 1,
    max_size: int = 10,
):
    global _pool
    if _pool is None:
        logger.info("[STREAM] Initializing asyncpg connection pool...")
        _pool = await asyncpg.create_pool(
            ASYNC_PG_URL,
            min_size=min_size,
            max_size=max_size,
        )
    return _pool


# ---------------------------------------------------------
# Stream rows one-by-one
# ---------------------------------------------------------
async def stream(
    sql: str,
    *params,
) -> AsyncGenerator[Dict[str, Any], None]:
    """
    Stream rows one at a time using asyncpg cursor.
    Yields Python dict rows.
    """
    pool = await init_asyncpg_pool()

    logger.info("[STREAM] Opening connection...")

    async with pool.acquire() as conn:
        async with conn.transaction():
            try:
                async for rec in conn.cursor(sql, *params):
                    yield dict(rec)
            finally:
                logger.info("[STREAM] Closing connection...")


# ---------------------------------------------------------
# Stream rows in batches
# ---------------------------------------------------------
async def stream_batches(
    sql: str,
    batch_size: int = 5000,
    *params,
) -> AsyncGenerator[List[Dict[str, Any]], None]:
    """
    Stream rows in batches.
    Each batch is a list of dict rows.
    """
    pool = await init_asyncpg_pool()

    logger.info("[STREAM BATCH] Opening connection...")

    async with pool.acquire() as conn:
        async with conn.transaction():
            cursor = conn.cursor(sql, *params)
            try:
                batch = []
                async for rec in cursor:
                    batch.append(dict(rec))

                    if len(batch) >= batch_size:
                        yield batch
                        batch = []
                # leftover rows
                if batch:
                    yield batch
            finally:
                logger.info("[STREAM BATCH] Closing connection...")


# ---------------------------------------------------------
# Close the pool (called on clean shutdown)
# ---------------------------------------------------------
async def close_asyncpg_pool():
    global _pool
    if _pool:
        logger.info("[STREAM] Closing asyncpg connection pool...")
        await _pool.close()
        _pool = None


# ---------------------------------------------------------
# Pool inspection tool
# ---------------------------------------------------------
async def inspect_pool(session: AsyncSession):
    """
    Print connection pool statistics for a SQLAlchemy async engine.
    Useful for debugging pool exhaustion, connection leaks,
    and async concurrency issues.
    """
    engine = session.get_bind()
    pool = engine.pool

    logger.info("=== Connection Pool Stats ===")
    logger.info("Checked in connections:", pool.checkedin())
    logger.info("Checked out connections:", pool.checkedout())
    logger.info("Overflow connections:", pool.overflow())
    logger.info("Pool size:", pool.size())
    logger.info("=============================")
