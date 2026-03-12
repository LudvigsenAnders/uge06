from ingestion import data_request
from models.pydantic_model import FeatureCollection, StationProperties, ObservationProperties
from .mapper import station_from_feature, observation_from_feature
from db.connection import get_session, close_engine, inspect_pool
from db.db_utils import QueryRunner
import httpx
from ingestion.repository import save_stations, save_observations
from typing import Optional, List, Dict, Tuple
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse
from helper_functions.helper_functions import save_checkpoint, load_checkpoint, clear_checkpoint

import logging
logger = logging.getLogger("ingestion")


def _process_feature(f, station_rows: List, observation_rows: List) -> None:
    """Process a single feature and append to appropriate row list."""
    props = f.properties
    if isinstance(props, StationProperties):
        station_rows.append(station_from_feature(f))
    elif isinstance(props, ObservationProperties):
        observation_rows.append(observation_from_feature(f))
    else:
        logger.error(f"Unknown properties type in feature {f.id}: {type(props)}")
        raise ValueError(f"Unknown properties type: {type(props)}")


def transform(pages: List[dict]) -> Tuple[List, List, int]:
    """Parse DMI FeatureCollection + map to ORM rows."""
    station_rows: List = []
    observation_rows: List = []
    total: int = 0

    for idx, raw in enumerate(pages, start=1):
        try:
            fc = FeatureCollection.model_validate(raw)
        except Exception as ex:
            logger.warning(f"Invalid page #{idx}: {ex}")
            continue

        total += len(fc.features)

        for f in fc.features:
            _process_feature(f, station_rows, observation_rows)

    logger.info(
        f"Parsed {total} features. "
        f"Stations: {len(station_rows)}, Observations: {len(observation_rows)}"
    )

    return station_rows, observation_rows, total


async def load_into_database(station_rows: List, observation_rows: List):
    """Write saved rows to DB"""
    async for session in get_session():
        q = QueryRunner(session)
        async with q.transaction():

            now = await q.fetch_value("SELECT NOW()")
            logger.debug(f"DB NOW(): {now}")

            if station_rows:
                logger.info(f"Saving {len(station_rows)} stations.")
                await save_stations(session, station_rows)

            if observation_rows:
                logger.info(f"Saving {len(observation_rows)} observations.")
                await save_observations(session, observation_rows)

    #await close_engine()


def canonicalize_url(url: str) -> str:
    """
    Normalize a URL for 'visited' tracking: scheme/host/path + sorted query.
    """
    parts = urlparse(url)
    query_pairs = parse_qsl(parts.query, keep_blank_values=True)
    query_sorted = urlencode(sorted(query_pairs))
    canonical = urlunparse((parts.scheme, parts.netloc, parts.path, '', query_sorted, ''))
    return canonical


def has_params_querystring(url: str) -> bool:
    return bool(urlparse(url).query)


async def _fetch_first_page(
    client: httpx.AsyncClient,
    start_url: str,
    base_params: dict,
) -> Tuple[Optional[dict], set]:
    """Fetch and process the first page of data."""
    logger.debug("Fetch first page: url=%s params=%s", start_url, base_params)

    page = await data_request.retry_async(
        data_request.request_data,
        client,
        start_url,
        base_params,
        retries=5,
        delay=1
    )
    if not page:
        logger.warning("[WARN] No first page response; aborting.")
        return None, set()

    url_with_params = (
        start_url
        if has_params_querystring(start_url)
        else f"{start_url}?{urlencode(sorted(base_params.items()))}"
    )
    first_canon = canonicalize_url(url_with_params)
    visited = {first_canon}

    return page, visited


async def _fetch_and_process_page(
    client: httpx.AsyncClient,
    next_url: str,
    visited: set,
    station_buf: list,
    obs_buf: list,
    total_features: int,
    flush_every: int,
) -> Tuple[str, int]:
    canon_next = canonicalize_url(next_url)

    if canon_next in visited:
        logger.info(f"Already visited next_url={next_url}; stopping to avoid duplicate page.")
        return None, total_features

    req_params = {} if not has_params_querystring(next_url) else None

    logger.info(f"Fetch next page: url={next_url} params={req_params}")

    page = await data_request.retry_async(
        data_request.request_data,
        client,
        next_url,
        req_params,
        retries=5,
        delay=1)

    if not page:
        logger.warning(f"[WARN] Failed to fetch page: {next_url}")
        return None, total_features

    visited.add(canon_next)

    stations, observations, n_features = transform([page])
    station_buf.extend(stations)
    obs_buf.extend(observations)
    total_features += n_features

    if len(station_buf) >= flush_every or len(obs_buf) >= flush_every:
        logger.info(f"Flushing {len(station_buf)} stations & {len(obs_buf)} observations.")
        await load_into_database(station_buf, obs_buf)

        await _save_checkpoint_to_database(next_url)

        station_buf.clear()
        obs_buf.clear()

    next_url = data_request.extract_next_link(page)
    return next_url, total_features


async def _save_checkpoint_to_database(next_url: str):
    if next_url:
        await save_checkpoint(next_url)
        logger.debug(f"[DEBUG] Saved checkpoint after DB flush: {next_url}")
    else:
        await clear_checkpoint()
    return


async def _process_pages(
    client: httpx.AsyncClient,
    next_url: str,
    visited: set,
    flush_every: int,
) -> Tuple[list, list, int]:
    """Process subsequent pages with pagination."""
    station_buf: list = []
    obs_buf: list = []
    total_features: int = 0

    while next_url:
        next_url, total_features = await _fetch_and_process_page(
            client, next_url, visited, station_buf, obs_buf, total_features, flush_every
        )

    return station_buf, obs_buf, total_features


async def _chech_for_features(page, n_features):

    if n_features == 0:
        logger.info("[INFO] No features returned for checkpoint URL -> end-of-stream. Clearing checkpoint and stopping.")
        await clear_checkpoint()
        return

    next_url = data_request.extract_next_link(page)

    if not next_url:
        logger.info("[INFO] No `next` link -> final page. Clearing checkpoint and stopping.")
        await clear_checkpoint()
        return



async def ingest_streaming(
    client: httpx.AsyncClient,
    start_url: str,
    base_params: dict,
    *,
    flush_every: int = 2000, 
    verbose: bool = True,
):
    # try resume
    resume_url = await load_checkpoint()
    if resume_url:
        logger.info(f"Resuming ingestion from saved URL: {resume_url}")
        url = resume_url
        params = None   # next links include their params
    else:
        url = start_url
        params = base_params
        logger.info(f"No checkpoint found. Starting fresh ingestion from initial URL: {start_url}")

    logger.info(f"Streaming ingest starting for {start_url}")

    station_buf: list = []
    obs_buf: list = []
    total_features: int = 0

    # ---- FIRST PAGE ----
    page, visited = await _fetch_first_page(client, url, params)
    if not page:
        return

    stations, observations, n_features = transform([page])
    await _chech_for_features(page, n_features)
    
    logger.info(f"length of station: {len(stations)} and length of obs: {len(observations)} amd n_feat: {n_features}" )
      
    station_buf.extend(stations)
    obs_buf.extend(observations)
    total_features += n_features

    # ---- NEXT PAGES ----
    next_url = data_request.extract_next_link(page)
    logger.info(f"Next URL extracted from first page: {next_url}")
    next_station, next_obs, next_total = await _process_pages(client, next_url, visited, flush_every)
    station_buf.extend(next_station)
    obs_buf.extend(next_obs)
    total_features += next_total

    # ---- FINAL FLUSH ----
    if station_buf or obs_buf:
        logger.info(f"Final flush: {len(station_buf)} stations & {len(obs_buf)} observations.")
        await load_into_database(station_buf, obs_buf)

        next_url = data_request.extract_next_link(page)
        await _save_checkpoint_to_database(next_url)

    logger.info(f"Streaming ingest completed. Total features processed: {total_features}")