import httpx
import asyncio
from typing import Optional
# from collections.abc import AsyncIterator
import logging

logger = logging.getLogger("data_request")


async def _make_request(client: httpx.AsyncClient, url: str, params: dict | None) -> httpx.Response:
    """Make HTTP GET request with proper parameter handling."""
    if params is None:
        # URL already contains everything (resume or next page)
        logger.debug("HTTP GET raw URL: %s", url)
        return await client.get(url)
    else:
        # Building a request using base URL + parameters
        logger.debug("HTTP GET with params: url=%s params=%s", url, params)
        return await client.get(url, params=params)


async def request_data(client: httpx.AsyncClient, url: str, params: dict | None):
    """
    Fetches a page from the API with correct handling:
    - If url already contains querystring → params MUST be None
    - If building URL for the first time → use params dict
    - Returns parsed JSON or None on failure
    """
    try:
        response = await _make_request(client, url, params)
        response.raise_for_status()
        return response.json()

    except httpx.TimeoutException:
        logger.error("Request timed out for URL: %s", url)

    except httpx.HTTPStatusError as exc:
        logger.error(
            "HTTP error %s for URL %s: %s",
            exc.response.status_code,
            exc.request.url,
            exc
        )

    except Exception as ex:
        logger.error("Unexpected error for URL %s: %s", url, ex)

    return None


# async def _fetch_one(client: httpx.AsyncClient, url: str, params: dict, max_concurrency: int) -> Optional[dict]:
#     sem = asyncio.Semaphore(max_concurrency)
#     async with sem:
#         return await request_data(client, url, params)


def extract_next_link(payload: dict) -> Optional[str]:
    """
    Extract 'next' link from a DMI FeatureCollection payload.
    Returns the next URL or None if not present.
    """
    try:
        links = payload.get("links", [])
        for link in links:
            if link.get("rel") == "next":
                return link.get("href")
    except Exception as ex:
        logger.warning(f"No next link {ex}")
        return None
    return None


async def retry_async(fn, *args, retries=5, delay=1, **kwargs):
    for attempt in range(1, retries + 1):
        try:
            logger.info(f"Attempt {attempt}/{retries} for function {fn.__name__}")
            return await fn(*args, **kwargs)
        except Exception as ex:
            logger.warning(f"[RETRY] Attempt {attempt}/{retries} failed: {ex}")
            if attempt == retries:
                raise BaseException(logger.error(f"All {retries} attempts failed for function {fn.__name__}")) from ex
            await asyncio.sleep(delay * attempt)


# async def stream_pages(client: httpx.AsyncClient, start_url: str, base_params: dict) -> AsyncIterator[dict]:
#     """Yield pages one by one, following 'next' links, fetching each page exactly once."""
#     # -------------------------
#     # Buffers for batched writes
#     # -------------------------
#     station_buf: list = []
#     obs_buf: list = []
#     total_features: int = 0

#     # -------------------------
#     # Streaming fetch loop
#     # -------------------------
#     next_url = start_url
#     params = base_params
#     visited = set()

#     # First page
#     page = await request_data(client, start_url, base_params)
#     if not page:
#         return
#     yield page
#     visited.add(start_url)


#     # After first page, do not send params again
#     params = {}


#     # Next pages
#     next_url = extract_next_link(page)
#     while next_url and next_url not in visited:
#         # You can still limit concurrency by firing the next N fetches ahead if desired
#         page = await request_data(client, next_url, params)
#         if not page:
#             break
#         yield page
#         visited.add(next_url)
#         next_url = extract_next_link(page)
