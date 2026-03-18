from __future__ import annotations
from typing import Optional, Dict, Any, List, Tuple, Set, Callable, AsyncContextManager
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse
import logging
import httpx
from etl_pipeline import data_request
from etl_pipeline.checkpoint_store import UrlCheckpointStore
from sqlalchemy.ext.asyncio import AsyncSession
from models.sqlalchemy_orm.observations import Observation
from models.sqlalchemy_orm.stations import Station
from models.pydantic_model import FeatureCollection, StationProperties, ObservationProperties, RecordsResponse
from .mapper import (station_from_feature_to_orm,
                     observation_from_feature_to_orm,
                     observations_from_bme280_to_ORM,
                     observations_from_DS18B20_to_ORM
                     )
from db.connection import get_session
from etl_pipeline.postgresql_repository import save_stations, save_observations


logger = logging.getLogger("etl_pipeline")


# ---- Helpers ----------------------------------------------------------------
def _canonicalize_url(url: str) -> str:
    """Canonicalize a URL by sorting query parameters for consistent comparison.

    Args:
        url: The URL to canonicalize.

    Returns:
        The canonicalized URL with sorted query parameters.
    """
    parts = urlparse(url)
    query_pairs = parse_qsl(parts.query, keep_blank_values=True)
    query_sorted = urlencode(sorted(query_pairs))
    return urlunparse((parts.scheme, parts.netloc, parts.path, '', query_sorted, ''))


def _build_url_with_params(url: str, params: Optional[Dict[str, Any]]) -> str:
    """Build a URL with query parameters, sorting them for consistency.

    Args:
        url: The base URL.
        params: Dictionary of query parameters to add.

    Returns:
        The URL with sorted query parameters appended.
    """
    if not params:
        return url
    query_sorted = urlencode(sorted(params.items()))
    parts = urlparse(url)
    return urlunparse((parts.scheme, parts.netloc, parts.path, '', query_sorted, ''))


def parse_spac_api(raw: dict) -> Tuple[List, List, int]:
    """Parse Specialisterne API response (RecordsResponse) into ORM objects.
    Takes raw JSON respone validates against the Pydantic RecordsResponse model and
    Return a Tuple with list of records

    Args:
        raw: Raw JSON response from Specialisterne API.

    Returns:
        Tuple of (stations, observations, total_count).
        Stations list is always empty for this API.
    """
    observations: list[Observation] = []
    try:
        rr = RecordsResponse.model_validate(raw)
    except Exception as ex:
        logger.warning("Invalid RecordsResponse: %s", ex)
        return [], [], 0

    # Convert each Record to ORM Observation rows
    for rec in rr.records:
        if hasattr(rec.reading, "BME280"):
            observations.extend(observations_from_bme280_to_ORM(rec))
        elif hasattr(rec.reading, "DS18B20"):
            observations.append(observations_from_DS18B20_to_ORM(rec))
        else:
            logger.warning("Unknown reading type: %s", type(rec.reading))
    return [], observations, len(observations)


def parse_dmi_api(raw: dict) -> Tuple[List, List, int]:
    """Parse DMI API response (FeatureCollection) into ORM objects.
    Takes raw JSON respone validates against the Pydantic FeatureCollection model and
    Return a Tuple with list of stations or list of observations
    Args:
        raw: Raw JSON response from DMI API.

    Returns:
        Tuple of (stations, observations, total_count).
    """
    stations: list[Station] = []
    observations: list[Observation] = []

    try:
        fc = FeatureCollection.model_validate(raw)
    except Exception as ex:
        logger.warning("Invalid page: %s", ex)
        return [], [], 0

    for f in fc.features:
        props = f.properties
        if isinstance(props, StationProperties):
            stations.append(station_from_feature_to_orm(f))
        elif isinstance(props, ObservationProperties):
            observations.append(observation_from_feature_to_orm(f))
        else:
            logger.warning("Unknown properties type in feature %s: %s", getattr(f, "id", "?"), type(props))

    return stations, observations, len(fc.features)


def transform_page(raw: dict) -> Tuple[List, List, int]:
    """
    Parse a FeatureCollection **or** a RecordsResponse and map to ORM rows.
    Returns (stations, observations, count).
    """
    stations: list[Station] = []
    observations: list[Observation] = []
    total: int = 0

    # --- Case 1: Specialisterne API (RecordsResponse) ---
    if "records" in raw:
        stations, observations, total = parse_spac_api(raw)
        # --- Case 2: FeatureCollection (GeoJSON) ---
    elif raw.get("type") == "FeatureCollection":
        stations, observations, total = parse_dmi_api(raw)
    else:
        # --- Unknown payload ---
        logger.warning("Unknown payload type: top-level keys=%s", list(raw.keys()))

    return stations, observations, total


# ---- Orchestrator -----------------------------------------------------------
class ETLPipeline:
    """
    Orchestrates paginated streaming ingestion:
      - Fetch pages with retry
      - Transform to ORM rows
      - Buffer & flush into DB
      - Save/clear checkpoints
      - Avoid duplicate page fetches (visited set)

    Dependencies are injected for testability.
    """

    def __init__(
        self,
        client: httpx.AsyncClient,
        session_factory: Callable[[], AsyncContextManager[AsyncSession]] = get_session,
        checkpoint: Optional[UrlCheckpointStore] = None,
        *,
        flush_every: int = 2000,
        logger_: Optional[logging.Logger] = None,
    ) -> None:
        self.client = client
        self.session_factory = session_factory
        self.checkpoint = checkpoint or UrlCheckpointStore(session_factory)
        self.flush_every = flush_every
        self.logger = logger_ or logger

        # internal state
        self._visited: Set[str] = set()
        self._station_buf: List = []
        self._obs_buf: List = []
        self._total_features: int = 0

    # ---- Public API ----

    async def run(self, start_url: str, base_params: Dict[str, Any]) -> int:
        """
        Returns total features processed.
        """
        url, params = await self._get_url_for_first_page(start_url, base_params)
        self.logger.info("ETL proces starting for %s", start_url)

        # First page
        page = await self._fetch_page(_build_url_with_params(url, params))

        if not page:
            return 0

        st, obs, n = transform_page(page)
        if not await self._should_continue(page, n):
            return 0

        self._extend_buffers(st, obs, n)

        # Ensure first page data is flushed before starting pagination
        if self._station_buf or self._obs_buf:
            await self._flush()

        next_url = data_request.extract_next_link(page)

        # Save checkpoint for first page if there's a next page
        if self.checkpoint and next_url:
            await self.checkpoint.save_checkpoint(next_url)

        self.logger.info("Next URL extracted from first page: %s", next_url)

        # Subsequent pages
        while next_url:
            next_url = await self._fetch_transform_and_maybe_flush(next_url)

        # Final flush
        await self._final_flush()
        self.logger.info("ETL proces completed. Total features processed: %s", self._total_features)
        return self._total_features

    # ---- Internals ----

    async def _get_url_for_first_page(
        self,
        start_url: str,
        base_params: Dict[str, Any]
    ) -> Tuple[str, Optional[Dict[str, Any]]]:
        """Determine the starting URL and parameters, checking for checkpoints.

        Args:
            start_url: The initial URL to start ingestion from.
            base_params: Base query parameters for the initial request.

        Returns:
            Tuple of (url, params) to use for the first page request.
            If resuming from checkpoint, params will be None.
        """
        if self.checkpoint:
            resume_url = await self.checkpoint.load_checkpoint()
            if resume_url:
                self.logger.info("Resuming ingestion from saved URL: %s", resume_url)
                return resume_url, None
        self.logger.info("No checkpoint found. Starting fresh ingestion from initial URL: %s", start_url)
        return start_url, base_params

    async def _fetch_page(self, url_with_params: str) -> Optional[dict]:
        """Fetch a page from the API with retry logic and duplicate detection.

        Args:
            url_with_params: The full URL with parameters to fetch.

        Returns:
            The JSON response as a dictionary, or None if failed or already visited.
        """
        canon = _canonicalize_url(url_with_params)
        if canon in self._visited:
            self.logger.info("Already visited %s; skipping.", url_with_params)
            return None

        self.logger.debug("Fetching url=%s", url_with_params)
        page = await data_request.retry_async(
            data_request.request_data,
            self.client,
            url_with_params,
            None,  # params none; url already normalized
            retries=5,
            delay=1
        )
        if page:
            self._visited.add(canon)
        else:
            self.logger.warning("Failed to fetch page: %s", url_with_params)
        return page

    async def _fetch_transform_and_maybe_flush(self, next_url: str) -> Optional[str]:
        """Fetch a page from a URL, transform data, and manage buffer flushing.

        This method ensures that checkpoints are saved only after all data
        from the current page has been successfully flushed to the database.

        Args:
            next_url: The URL of the next page to process.

        Returns:
            The URL of the following page, or None if no more pages.
        """
        page = await self._fetch_page(next_url)
        if not page:
            return None

        st, obs, n = transform_page(page)
        self._extend_buffers(st, obs, n)

        # Flush if buffers exceed the threshold (memory management)
        if self._should_flush():
            await self._flush()

        # Ensure all buffered data is flushed before saving checkpoint
        # This prevents data loss if we crash after saving checkpoint but before flushing
        if self._station_buf or self._obs_buf:
            await self._flush()

        # Save checkpoint only after ensuring no buffered data remains
        if self.checkpoint:
            nxt = data_request.extract_next_link(page)
            if nxt:
                await self.checkpoint.save_checkpoint(nxt)

        return data_request.extract_next_link(page)

    def _extend_buffers(self, stations: List, observations: List, n_features: int) -> None:
        """Add parsed data to internal buffers and update counters.

        Args:
            stations: List of station ORM objects to buffer.
            observations: List of observation ORM objects to buffer.
            n_features: Number of features processed.
        """
        self._station_buf.extend(stations)
        self._obs_buf.extend(observations)
        self._total_features += n_features

    def _should_flush(self) -> bool:
        """Check if buffers should be flushed to the database.

        Returns:
            True if any buffer exceeds the flush threshold.
        """
        total = len(self._station_buf) + len(self._obs_buf)
        return total >= self.flush_every or \
            len(self._station_buf) >= self.flush_every or \
            len(self._obs_buf) >= self.flush_every

    async def _flush(self) -> None:
        """Flush buffered data to the database.

        Saves all buffered stations and observations to the database
        and clears the buffers. Also saves checkpoint if available.
        """
        if not self._station_buf and not self._obs_buf:
            return
        self.logger.info("Flushing %s stations & %s observations.", len(self._station_buf), len(self._obs_buf))
        async with self.session_factory() as session:
            async with session.begin():
                if self._station_buf:
                    await save_stations(session, self._station_buf)
                if self._obs_buf:
                    await save_observations(session, self._obs_buf)
        self._station_buf.clear()
        self._obs_buf.clear()

    async def _final_flush(self) -> None:
        """Perform final flush and clear checkpoints.

        Flushes any remaining buffered data and clears the checkpoint
        to indicate completion.
        """
        await self._flush()
        if self.checkpoint:
            await self.checkpoint.clear_checkpoint()


    async def _should_continue(self, page: dict, n_features: int) -> bool:
        """Determine if ingestion should continue based on page content.

        Args:
            page: The fetched page data.
            n_features: Number of features in the page.

        Returns:
            True if processing should continue, False if it should stop.
        """
        if n_features == 0:
            self.logger.info("No features returned for checkpoint URL -> end-of-stream.")
            if self.checkpoint:
                await self.checkpoint.clear_checkpoint()
            return False

        next_url = data_request.extract_next_link(page)
        if not next_url:
            self.logger.info("No `next` link -> final page.")
            if self.checkpoint:
                await self.checkpoint.clear_checkpoint()
            return True

        return True
