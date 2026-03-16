
# ingestion/checkpoint_store.py
from typing import Optional
from sqlalchemy import text

# TODO: remove create new session on save and clear checkpoint both here and in etl_pipeline


class UrlCheckpointStore:
    def __init__(self, session_factory):
        self.session_factory = session_factory

    async def load_checkpoint(self) -> Optional[str]:
        async with self.session_factory() as session:
            result = await session.execute(text("SELECT next_url FROM ingest_checkpoint WHERE id=1"))
            url: str = result.scalar_one_or_none()
            return url

    async def save_checkpoint(self, url: str) -> None:
        async with self.session_factory() as session:
            await session.execute(text(
                """
                INSERT INTO ingest_checkpoint (id, next_url, created)
                VALUES (1, :url ,NOW())
                ON CONFLICT (id) DO UPDATE
                SET next_url = :url"""),
                {"url": url}
            )

    async def clear_checkpoint(self) -> None:
        print("CLEARING CHECKPOINT")

        async with self.session_factory() as session:
            await session.execute(text("DELETE FROM ingest_checkpoint WHERE id = 1"))
            await session.commit()
