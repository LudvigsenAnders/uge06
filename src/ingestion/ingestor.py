# ingestion/streaming.py

from __future__ import annotations

from typing import Optional, List, Tuple, Set, Dict, Any
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse

import logging
import httpx

from ingestion import data_request
from models.pydantic_model import FeatureCollection, StationProperties, ObservationProperties
from .mapper import station_from_feature, observation_from_feature
from db.connection import get_session
from db.db_utils import QueryRunner
from ingestion.repository import save_stations, save_observations
from helper_functions.helper_functions import save_checkpoint, load_checkpoint, clear_checkpoint

logger = logging.getLogger("ingestion")


def _canonicalize_url(url: str) -> str:
    """
    Normalize a URL for 'visited' tracking: scheme/host/path + sorted query.
    NOTE: keeps blank query values.
    """
    parts = urlparse(url)
    query_pairs = parse_qsl(parts.query, keep_blank_values=True)
    query_sorted = urlencode(sorted(query_pairs))
    return urlunparse((parts.scheme, parts.netloc, parts.path, '', query_sorted, ''))


def _build_url_with_params(url: str, params: Optional[Dict[str, Any]]) -> str:
    """
    Build a URL with sorted query parameters.
    If params is None, return the URL as-is.
    """
    if not params:
        return url
    query_sorted = urlencode(sorted(params.items()))
    parts = urlparse(url)
    return urlunparse((parts.scheme, parts.netloc, parts.path, '', query_sorted, ''))


def _has_querystring(url: str) -> bool:
    return bool(urlparse(url).query)


def _append_feature_to_list(feature, station_rows: List, observation_rows: List) -> None:
    """
    Checks a single feature with either StationProperties or ObservationProperties
    and append to appropriate row list.
    """
    props = feature.properties
    if isinstance(props, StationProperties):
        station_rows.append(station_from_feature(feature))
    elif isinstance(props, ObservationProperties):
        observation_rows.append(observation_from_feature(feature))
    else:
        # Prefer resilience in ingestion pipelines
        logger.warning("Unknown properties type in feature %s: %s", getattr(feature, "id", "?"), type(props))


def transform(pages: List[dict]) -> Tuple[List, List, int]:
    """Parse DMI FeatureCollection + map to ORM rows."""
    station_rows: List = []
    observation_rows: List = []
    total: int = 0

    for idx, raw in enumerate(pages, start=1):
        try:
            fc = FeatureCollection.model_validate(raw)
        except Exception as ex:
            logger.warning("Invalid page #%s: %s", idx, ex)
            continue

        total += len(fc.features)

        for f in fc.features:
            _append_feature_to_list(f, station_rows, observation_rows)

    logger.info(
        "Parsed %s features. Stations: %s, Observations: %s",
        total, len(station_rows), len(observation_rows)
    )

    return station_rows, observation_rows, total


async def load_into_database(station_rows: List, observation_rows: List):
    """Write saved rows to DB (in a single transaction)."""
    async for session in get_session():
        q = QueryRunner(session)
        async with q.transaction():
            now = await q.fetch_value("SELECT NOW()")
            logger.debug("DB NOW(): %s", now)

            if station_rows:
                logger.info("Saving %s stations.", len(station_rows))
                await save_stations(session, station_rows)

            if observation_rows:
                logger.info("Saving %s observations.", len(observation_rows))
                await save_observations(session, observation_rows)


async def _fetch_first_page(
    client: httpx.AsyncClient,
    url: str,
    params: Optional[Dict[str, Any]],
) -> Tuple[Optional[dict], Set[str]]:
    """Fetch and process the first page of data."""
    normalized_url = _build_url_with_params(url, params)
    logger.debug("Fetch first page: url=%s", normalized_url)

    page = await data_request.retry_async(
        data_request.request_data,
        client,
        normalized_url,
        None,            # params=None since normalized_url already contains query
        retries=5,
        delay=1
    )
    if not page:
        logger.warning("No first page response; aborting.")
        return None, set()

    visited = {_canonicalize_url(normalized_url)}
    return page, visited


async def _save_checkpoint(next_url: Optional[str]) -> None:
    if next_url:
        await save_checkpoint(next_url)
        logger.debug("Saved checkpoint after DB flush: %s", next_url)
    else:
        await clear_checkpoint()


async def _fetch_and_process_page(
    client: httpx.AsyncClient,
    next_url: str,
    visited: Set[str],
    station_buf: List,
    obs_buf: List,
    total_features: int,
    flush_every: int,
) -> Tuple[Optional[str], int]:
    canon_next = _canonicalize_url(next_url)

    if canon_next in visited:
        logger.info("Already visited next_url=%s; stopping to avoid duplicate page.", next_url)
        return None, total_features

    logger.info("Fetch next page: url=%s", next_url)
    page = await data_request.retry_async(
        data_request.request_data,
        client,
        next_url,
        None,  # next links should include their own params
        retries=5,
        delay=1
    )

    if not page:
        logger.warning("Failed to fetch page: %s", next_url)
        return None, total_features

    visited.add(canon_next)

    stations, observations, n_features = transform([page])
    station_buf.extend(stations)
    obs_buf.extend(observations)
    total_features += n_features

    # Flush by combined size or if any exceeds threshold
    if (len(station_buf) + len(obs_buf)) >= flush_every or \
       len(station_buf) >= flush_every or len(obs_buf) >= flush_every:
        logger.info("Flushing %s stations & %s observations.", len(station_buf), len(obs_buf))
        await load_into_database(station_buf, obs_buf)
        await _save_checkpoint(data_request.extract_next_link(page))
        station_buf.clear()
        obs_buf.clear()

    return data_request.extract_next_link(page), total_features


async def _process_pages(
    client: httpx.AsyncClient,
    next_url: Optional[str],
    visited: Set[str],
    flush_every: int,
) -> Tuple[List, List, int]:
    """Process subsequent pages with pagination."""
    station_buf: List = []
    obs_buf: List = []
    total_features: int = 0

    while next_url:
        next_url, total_features = await _fetch_and_process_page(
            client, next_url, visited, station_buf, obs_buf, total_features, flush_every
        )

    return station_buf, obs_buf, total_features


async def _check_for_features(page: dict, n_features: int) -> bool:
    """
    Returns False if the stream should stop (no features and/or no next link), True otherwise.
    Also manages checkpoint clearing when stream is exhausted.
    """
    if n_features == 0:
        logger.info("No features returned for checkpoint URL -> end-of-stream. Clearing checkpoint and stopping.")
        await clear_checkpoint()
        return False

    next_url = data_request.extract_next_link(page)
    if not next_url:
        logger.info("No `next` link -> final page. Clearing checkpoint and stopping.")
        await clear_checkpoint()
        return False

    return True


async def _get_url_for_first_page(start_url: str, base_params: Dict[str, Any]) -> Tuple[str, Optional[Dict[str, Any]]]:
    resume_url = await load_checkpoint()
    if resume_url:
        logger.info("Resuming ingestion from saved URL: %s", resume_url)
        return resume_url, None   # next links include their params
    logger.info("No checkpoint found. Starting fresh ingestion from initial URL: %s", start_url)
    return start_url, base_params


async def ingest_streaming(
    client: httpx.AsyncClient,
    start_url: str,
    base_params: Dict[str, Any],
    *,
    flush_every: int = 2000,
) -> None:

    url, params = await _get_url_for_first_page(start_url, base_params)
    logger.info("Streaming ingest starting for %s", start_url)

    station_buf: List = []
    obs_buf: List = []
    total_features: int = 0

    # ---- FIRST PAGE ----
    page, visited = await _fetch_first_page(client, url, params)
    if not page:
        return

    stations, observations, n_features = transform([page])
    if not await _check_for_features(page, n_features):
        # Already cleared checkpoint inside _check_for_features
        return

    logger.info("Stations=%s, Observations=%s, n_features=%s", len(stations), len(observations), n_features)

    station_buf.extend(stations)
    obs_buf.extend(observations)
    total_features += n_features

    # ---- NEXT PAGES ----
    next_url = data_request.extract_next_link(page)
    logger.info("Next URL extracted from first page: %s", next_url)
    next_station, next_obs, next_total = await _process_pages(client, next_url, visited, flush_every)

    station_buf.extend(next_station)
    obs_buf.extend(next_obs)
    total_features += next_total

    # ---- FINAL FLUSH ----
    if station_buf or obs_buf:
        logger.info("Final flush: %s stations & %s observations.", len(station_buf), len(obs_buf))
        await load_into_database(station_buf, obs_buf)
        await clear_checkpoint()

    logger.info("Streaming ingest completed. Total features processed: %s", total_features)
