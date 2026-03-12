
import asyncio
from typing import Dict, List, Optional
import httpx


# -----------------------------
# Concurrency configuration
# -----------------------------
MAX_CONCURRENCY = 5
SEM = asyncio.Semaphore(MAX_CONCURRENCY)


async def request_data(client: httpx.AsyncClient, url: str, parameters: dict):
    try:
        response = await client.get(url, params=parameters)
        response.raise_for_status()
        return response.json()
    except httpx.TimeoutException:
        print("Request timed out!")
    except httpx.HTTPStatusError as exc:
        print(f"HTTP error {exc.response.status_code}: {exc}")
    return None


def _extract_next_link(payload: dict) -> Optional[str]:
    for link in payload.get("links", []):
        if link.get("rel") == "next":
            return link.get("href")
    return None

async def _fetch_one(client: httpx.AsyncClient, url: str, params: Dict):
    async with SEM:
        return await request_data(client, url, params)

async def gather_all_pages_streaming(client: httpx.AsyncClient, start_url: str, base_params: Dict) -> List[dict]:
    pages: List[dict] = []
    queue: asyncio.Queue[tuple[str, Dict]] = asyncio.Queue()

    # Seed with first request
    await queue.put((start_url, base_params))

    in_flight: set[asyncio.Task] = set()

    async def worker():
        while True:
            try:
                url, params = await queue.get()
            except asyncio.CancelledError:
                return
            try:
                payload = await _fetch_one(client, url, params)
                if payload:
                    pages.append(payload)
                    nxt = _extract_next_link(payload)
                    if nxt:
                        # next has full href, use empty params to avoid duplication
                        await queue.put((nxt, {}))
            finally:
                queue.task_done()

    # Start a limited number of workers
    workers = [asyncio.create_task(worker()) for _ in range(MAX_CONCURRENCY)]

    # Wait until queue is empty and all tasks done
    await queue.join()

    # Cancel workers
    for w in workers:
        w.cancel()
    await asyncio.gather(*workers, return_exceptions=True)

    return pages
