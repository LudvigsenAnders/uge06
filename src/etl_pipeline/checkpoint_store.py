from typing import Optional
from sqlalchemy import text

# TODO: remove create new session on save and clear checkpoint both here and in etl_pipeline


class UrlCheckpointStore:
    """Manages URL checkpoints for resumable ETL ingestion.

    Stores the next URL to process in the database, allowing the ETL pipeline
    to resume from interruptions without reprocessing already completed data.

    TODO: Remove the start of new session factory from URLCheckpointStore and refactor
    etl_pipeline._fetch_transform_and_maybe_flush so that, await self.checkpoint.save_checkpoint(nxt)
    is inside etl_pipeline._flush instead.


    """

    def __init__(self, session_factory):
        """Initialize the checkpoint store with a database session factory.

        Args:
            session_factory: Callable that returns an async database session.
        """
        self.session_factory = session_factory

    async def load_checkpoint(self) -> Optional[str]:
        """Load the saved checkpoint URL from the database.

        Returns:
            The next URL to process, or None if no checkpoint exists.
        """
        async with self.session_factory() as session:
            result = await session.execute(text("SELECT next_url FROM ingest_checkpoint WHERE id=1"))
            url: str = result.scalar_one_or_none()
            return url

    async def save_checkpoint(self, url: str) -> None:
        """Save a checkpoint URL to the database.

        Args:
            url: The next URL to process, to be saved as checkpoint.
        """
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
        """Clear the saved checkpoint from the database.

        Called when ETL processing is complete to prevent resuming
        from an outdated checkpoint.
        """
        print("CLEARING CHECKPOINT")

        async with self.session_factory() as session:
            await session.execute(text("DELETE FROM ingest_checkpoint WHERE id = 1"))
            await session.commit()
