
# ingestion/checkpoint_store.py
from typing import Optional
from helper_functions.helper_functions import save_checkpoint, load_checkpoint, clear_checkpoint


class UrlCheckpointStore:
    async def load(self) -> Optional[str]:
        return await load_checkpoint()  # returns str | None

    async def save(self, next_url: str) -> None:
        await save_checkpoint(next_url)  # ensure this saves only the URL

    async def clear(self) -> None:
        await clear_checkpoint()
