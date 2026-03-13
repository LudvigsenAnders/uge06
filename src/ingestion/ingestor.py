from __future__ import annotations

from typing import Optional, Dict, Any, List, Tuple, Set, Protocol
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse
import logging
import httpx

from ingestion import data_request
from models.pydantic_model import FeatureCollection, StationProperties, ObservationProperties, RecordsResponse, Record
from .mapper import station_from_feature, observation_from_feature, record_to_observation_orm
from db.connection import get_session
from db.db_utils import QueryRunner
from ingestion.repository import save_stations, save_observations

logger = logging.getLogger("ingestion")


# ---- Protocols / Interfaces for DI -----------------------------------------

class CheckpointStore(Protocol):
    async def load(self) -> Optional[str]:
        pass

    async def save(self, next_url: str) -> None:
        pass

    async def clear(self) -> None:
        pass


class SessionFactory(Protocol):
    def __aiter__(self):
        pass

    async def __anext__(self):
        pass

# ---- Helpers ----------------------------------------------------------------


def _canonicalize_url(url: str) -> str:
    parts = urlparse(url)
    query_pairs = parse_qsl(parts.query, keep_blank_values=True)
    query_sorted = urlencode(sorted(query_pairs))
    return urlunparse((parts.scheme, parts.netloc, parts.path, '', query_sorted, ''))


def _build_url_with_params(url: str, params: Optional[Dict[str, Any]]) -> str:
    if not params:
        return url
    query_sorted = urlencode(sorted(params.items()))
    parts = urlparse(url)
    return urlunparse((parts.scheme, parts.netloc, parts.path, '', query_sorted, ''))


def transform_page(raw: dict) -> Tuple[List, List, int]:
    
    """
    Parse a FeatureCollection **or** a RecordsResponse and map to ORM rows.
    Returns (stations, observations, count).
    """

    
    # --- Case 1: Specialisterne API (RecordsResponse) ---
    if "records" in raw:
        try:
            rr = RecordsResponse.model_validate(raw)
        except Exception as ex:
            logger.warning("Invalid RecordsResponse: %s", ex)
            return [], [], 0

        # Convert each Record to ORM rows
        # You need your own mapping functions here
        observations = [record_to_observation_orm(rec) for rec in rr.records]
        
        return [], observations, len(rr.records)



    # --- Case 2: FeatureCollection (GeoJSON) ---
    if raw.get("type") == "FeatureCollection":

        try:
            fc = FeatureCollection.model_validate(raw)
        except Exception as ex:
            logger.warning("Invalid page: %s", ex)
            return [], [], 0

        stations: List = []
        observations: List = []
        for f in fc.features:
            props = f.properties
            if isinstance(props, StationProperties):
                stations.append(station_from_feature(f))
            elif isinstance(props, ObservationProperties):
                observations.append(observation_from_feature(f))
            else:
                logger.warning("Unknown properties type in feature %s: %s", getattr(f, "id", "?"), type(props))

        return stations, observations, len(fc.features)
        
    # --- Unknown payload ---
    logger.warning("Unknown payload type: top-level keys=%s", list(raw.keys()))
    return [], [], 0


# ---- Orchestrator -----------------------------------------------------------
class StreamingIngestor:
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
        session_factory: SessionFactory = get_session,
        checkpoint: Optional[CheckpointStore] = None,
        *,
        flush_every: int = 2000,
        logger_: Optional[logging.Logger] = None,
    ) -> None:
        self.client = client
        self.session_factory = session_factory
        self.checkpoint = checkpoint
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
        self.logger.info("Streaming ingest starting for %s", start_url)

        # First page
        page = await self._fetch_page(_build_url_with_params(url, params))
        if not page:
            return 0

        st, obs, n = transform_page(page)
        if not await self._should_continue(page, n):
            return 0

        self._extend_buffers(st, obs, n)
        next_url = data_request.extract_next_link(page)
        self.logger.info("Next URL extracted from first page: %s", next_url)

        # Subsequent pages
        while next_url:
            next_url = await self._fetch_transform_and_maybe_flush(next_url)

        # Final flush
        await self._final_flush()
        self.logger.info("Streaming ingest completed. Total features processed: %s", self._total_features)
        return self._total_features

    # ---- Internals ----

    async def _get_url_for_first_page(
        self,
        start_url: str,
        base_params: Dict[str, Any]
    ) -> Tuple[str, Optional[Dict[str, Any]]]:
        if self.checkpoint:
            resume_url = await self.checkpoint.load()
            if resume_url:
                self.logger.info("Resuming ingestion from saved URL: %s", resume_url)
                return resume_url, None
        self.logger.info("No checkpoint found. Starting fresh ingestion from initial URL: %s", start_url)
        return start_url, base_params

    async def _fetch_page(self, url_with_params: str) -> Optional[dict]:
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
        page = await self._fetch_page(next_url)
        if not page:
            return None

        st, obs, n = transform_page(page)
        self._extend_buffers(st, obs, n)

        if self._should_flush():
            await self._flush()
            if self.checkpoint:
                nxt = data_request.extract_next_link(page)
                if nxt:
                    await self.checkpoint.save(nxt)

        return data_request.extract_next_link(page)

    def _extend_buffers(self, stations: List, observations: List, n_features: int) -> None:
        self._station_buf.extend(stations)
        self._obs_buf.extend(observations)
        self._total_features += n_features

    def _should_flush(self) -> bool:
        total = len(self._station_buf) + len(self._obs_buf)
        return total >= self.flush_every or \
            len(self._station_buf) >= self.flush_every or \
            len(self._obs_buf) >= self.flush_every

    async def _flush(self) -> None:
        if not self._station_buf and not self._obs_buf:
            return
        self.logger.info("Flushing %s stations & %s observations.", len(self._station_buf), len(self._obs_buf))
        async for session in self.session_factory():
            q = QueryRunner(session)
            async with q.transaction():
                if self._station_buf:
                    await save_stations(session, self._station_buf)
                if self._obs_buf:
                    await save_observations(session, self._obs_buf)
        self._station_buf.clear()
        self._obs_buf.clear()

    async def _final_flush(self) -> None:
        await self._flush()
        if self.checkpoint:
            await self.checkpoint.clear()

    async def _should_continue(self, page: dict, n_features: int) -> bool:
        if n_features == 0:
            self.logger.info("No features returned for checkpoint URL -> end-of-stream.")
            if self.checkpoint:
                await self.checkpoint.clear()
            return False

        next_url = data_request.extract_next_link(page)
        if not next_url:
            self.logger.info("No `next` link -> final page.")
            if self.checkpoint:
                await self.checkpoint.clear()
            return False

        return True
