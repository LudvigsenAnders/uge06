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
