"""
Main entry point for the meteorological data ETL and analysis pipeline.

This module orchestrates the extraction, transformation, and loading (ETL) of
meteorological data from DMI APIs and performs data analysis on the collected
observations.
"""

from helper_functions.helper_functions import setup_logging
import httpx
import sys
import asyncio
from typing import Callable, Any
from db.connection import MY_TOKEN, get_session
from db.init_db import init_db
from db.db_utils import QueryRunner
from etl_pipeline.etl_pipeline import ETLPipeline
from data_analysis.dataframe_repository import AsyncObservationRepository
from data_analysis.dataframe_analysis_service import ObservationAnalysisService
from helper_functions.helper_functions import parse_dt, month_range_to_datetime


# --- Windows fix for asyncpg (required on Windows) ---
if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


LOGGING = True
MAX_CONCURRENCY = 5


async def etl():
    """Run the ETL pipeline to fetch and process meteorological data.

    Fetches station and observation data from DMI APIs, transforms it,
    and loads it into the database. Uses checkpointing to resume from
    interruptions.
    """
    setup_logging(LOGGING)
    await init_db()

    station_url = "https://opendataapi.dmi.dk/v2/metObs/collections/station/items"
    station_parameters = {}

    met_obs_period, start_date, end_date = month_range_to_datetime("2020-07", "2020-12")

    met_obs_url = "https://opendataapi.dmi.dk/v2/metObs/collections/observation/items"
    met_obs_parameters = {
        "datetime": met_obs_period,  # "2025-01-01T00:00:00Z/2025-01-31T00:00:00Z"
        "stationId": "06072",  # Ødum: "06072", Årslev: "06126", Landbohøjskolen: "06186"
        "parameterId": "temp_dry",
        "limit": 5000,
        "sortorder": "observed,DESC"
    }

    spac_url = "https://climate.spac.dk/api/records"
    spac_parameters = {
        "from": "2025-01-01T00:00:00Z",
        "limit": "1000000"
    }
    async with httpx.AsyncClient(timeout=30, headers={"Authorization": f"Bearer {MY_TOKEN}"}) as client:
        pipeline = ETLPipeline(
            client=client,
            session_factory=get_session,
            flush_every=2000
        )
        total = await pipeline.run(
            start_url=met_obs_url,
            base_params=met_obs_parameters
        )
        print(f"ETL proces completed: {total} rows processed")


async def analysis_service(station_id: str, start_date: str, end_date: str) -> None:
    """Run data analysis examples for a specific weather station.

    Demonstrates various data analysis capabilities including:
    - Raw observation data retrieval
    - Daily statistics aggregation
    - Anomaly detection using z-score
    - Data completeness reporting
    - Data visualization

    Args:
        station_id: The identifier of the weather station to analyze.
    """
    print("NOW IN DATAFRAME RUNNER")
    session_factory = get_session

    async with session_factory() as session:

        q = QueryRunner(session)
        repo = AsyncObservationRepository(q)
        svc = ObservationAnalysisService(repo)

        # Example 1: Data access only
        df_obs = await repo.get_observations_multi_station(
            station_ids=[station_id],
            parameter_id="temp_dry",
            since=parse_dt(start_date),
            until=parse_dt(end_date)
        )
        print("Raw observations:", df_obs.info())

        # Example 2: Daily stats
        daily = await svc.daily_stats_for_station(
            station_id=station_id,
            parameter_id="temp_dry",
            since=parse_dt(start_date),
            until=parse_dt(end_date),
            agg="mean",
        )
        print(daily.head())

        # Example 3: Anomalies
        anomalies = await svc.anomalies_zscore_for_station(
            station_id=station_id,
            parameter_id="temp_dry",
            since=parse_dt(start_date),
            until=parse_dt(end_date),
            z_threshold=3.0,
            rolling="14D",
        )
        #print(anomalies.head())

        # Example 4: Completeness
        completeness = await svc.completeness_report(
            station_id=station_id,
            parameter_id="temp_dry",
            since=parse_dt(start_date),
            until=parse_dt(end_date),
            frequency="1h",
        )
        #print(completeness)

        svc.plotter(df_obs)


SEM = asyncio.Semaphore(2)


async def guarded(fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    """Execute a function with concurrency control using a semaphore.

    Limits concurrent execution to prevent overwhelming the database
    or external APIs.

    Args:
        fn: The async function to execute.
        *args: Positional arguments for the function.
        **kwargs: Keyword arguments for the function.

    Returns:
        The result of the function execution.
    """
    async with SEM:
        return await fn(*args, **kwargs)


async def main() -> None:
    """Main entry point that runs analysis for multiple stations in parallel.

    Demonstrates concurrent processing of multiple weather stations
    with controlled concurrency to avoid resource exhaustion.
    """
    met_obs_period, start_date, end_date = month_range_to_datetime("2020-01", "2020-12")
    
    print(start_date, end_date)
    
    station_ids = ["06072", "06126", "06186"]
    await etl()
    # results = await asyncio.gather(*(analysis_service(s) for s in station_ids))
    results = await asyncio.gather(*(guarded(analysis_service, s, start_date, end_date) for s in station_ids))
    print("Parallel finished:")

if __name__ == "__main__":
    asyncio.run(main())
