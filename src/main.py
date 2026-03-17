from helper_functions.helper_functions import setup_logging
import httpx
import sys
import asyncio
from db.connection import MY_TOKEN, get_session
from db.init_db import init_db
from db.db_utils import QueryRunner
from etl_pipeline.etl_pipeline import ETLPipeline
from data_analysis.dataframe_repository import AsyncObservationRepository
from data_analysis.dataframe_analysis_service import ObservationAnalysisService
from helper_functions.helper_functions import parse_dt


# --- Windows fix for asyncpg (required on Windows) ---
if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


LOGGING = True
MAX_CONCURRENCY = 5


async def etl():

    setup_logging(LOGGING)
    await init_db()

    station_url = "https://opendataapi.dmi.dk/v2/metObs/collections/station/items"
    station_parameters = {}

    met_obs_url = "https://opendataapi.dmi.dk/v2/metObs/collections/observation/items"
    met_obs_parameters = {
        "datetime": "2018-01-01T00:00:00Z/2018-01-31T00:00:00Z",
        #"stationId": "06072",
        "parameterId": "temp_dry",
        #"limit": 10,
        "sortorder": "observed,DESC"
    }

    spac_url = "https://climate.spac.dk/api/records"
    spac_parameters = {
        #"from": "2026-02-27T09:32:45Z",
        "limit": "250"
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


async def analysis_service(station_id: str):
    print("NOW IN DATAFRAME RUNNER")
    session_factory = get_session

    async with session_factory() as session:

        q = QueryRunner(session)
        repo = AsyncObservationRepository(q)
        svc = ObservationAnalysisService(repo)

        # Example 1: Data access only
        df_obs = await repo.get_observations_multi_station(
            station_ids=[station_id],
            since=parse_dt("2018-01-01T00:00:00Z"),
            until=parse_dt("2018-03-31T00:00:00Z")
        )
        print("Raw observations:", df_obs.info())

        # Example 2: Daily stats
        daily = await svc.daily_stats_for_station(
            station_id=station_id,
            parameter_id="temp_dry",
            since=parse_dt("2018-01-01T00:00:00Z"),
            until=parse_dt("2018-03-31T00:00:00Z"),
            agg="mean",
        )
        print(daily.head())

        # Example 3: Anomalies
        anomalies = await svc.anomalies_zscore_for_station(
            station_id=station_id,
            parameter_id="temp_dry",
            since=parse_dt("2018-01-01T00:00:00Z"),
            until=parse_dt("2018-03-31T00:00:00Z"),
            z_threshold=3.0,
            rolling="14D",
        )
        print(anomalies.head())

        # Example 4: Completeness
        completeness = await svc.completeness_report(
            station_id=station_id,
            parameter_id="temp_dry",
            since=parse_dt("2018-01-01T00:00:00Z"),
            until=parse_dt("2018-03-31T00:00:00Z"),
            frequency="1h",
        )
        print(completeness)

        svc.plotter(df_obs)


SEM = asyncio.Semaphore(2)


async def guarded(fn, *args, **kwargs):
    async with SEM:
        return await fn(*args, **kwargs)


async def main():
    station_ids = ["06072", "06073", "06074"]
    #await etl()
    #result = await analysis_service()
    #results = await asyncio.gather(*(analysis_service(s) for s in station_ids))
    results = await asyncio.gather(*(guarded(analysis_service, s) for s in station_ids))
    print("Parallel finished:")

if __name__ == "__main__":
    asyncio.run(main())
