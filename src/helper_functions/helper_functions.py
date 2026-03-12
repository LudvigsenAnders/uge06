import logging
import ingestion.data_request
from sqlalchemy import text
from db import connection
from db.db_utils import QueryRunner
from models.pydantic_model import FeatureCollection, StationProperties, ObservationProperties 



def setup_logging(enabled: bool = True):
    if logging.getLogger().hasHandlers():
        return  # prevent duplicate handlers

    if not enabled:
        logging.disable(logging.CRITICAL)
        return

    # Level string set to DEBUG
    level = "DEBUG"
    level = getattr(logging, level.upper(), logging.DEBUG)
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        force=True,  # <-- critical to override prior handlers
    )


async def save_checkpoint(page):
    url = ingestion.data_request.extract_next_link(page)

    async for session in connection.get_session():
        q = QueryRunner(session)
        async with q.transaction():
            await session.execute(text(
                """
                INSERT INTO ingest_checkpoint (id, next_url, created)
                VALUES (1, :url ,NOW())
                ON CONFLICT (id) DO UPDATE
                SET next_url = :url"""),
                {"url": url}
            )
        await session.commit()
    #await connection.close_engine()


async def load_checkpoint():
    async for session in connection.get_session():
        q = QueryRunner(session)
        async with q.transaction():
            result = await session.execute(text("SELECT next_url FROM ingest_checkpoint WHERE id=1"))
            # result.scalar_one_or_none() returns the *first column* of the row or None
            url = result.scalar_one_or_none()
            return url  # Already a str or None
    #await connection.close_engine()


async def clear_checkpoint():
    async for session in connection.get_session():
        q = QueryRunner(session)
        async with q.transaction():
            await session.execute(text("DELETE FROM ingest_checkpoint WHERE id = 1"))
        await session.commit()
    #await connection.close_engine()