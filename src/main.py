from helper_functions.helper_functions import setup_logging
import httpx
import asyncio
from db.connection import MY_TOKEN
from db.init_db import init_db
from etl_pipeline.etl_pipeline import ETLPipeline
from etl_pipeline.checkpoint_store import UrlCheckpointStore

LOGGING = True
MAX_CONCURRENCY = 5


async def main():

    setup_logging(LOGGING)

    print(f"Using MAX_CONCURRENCY={MAX_CONCURRENCY}")

    await init_db()

    station_url = "https://opendataapi.dmi.dk/v2/metObs/collections/station/items"
    station_parameters = {
        #"limit": 10,
        #"offset": 0,
    }

    met_obs_url = "https://opendataapi.dmi.dk/v2/metObs/collections/observation/items"
    met_obs_parameters = {
        "datetime": "2018-01-01T00:00:00Z/2018-03-31T00:00:00Z",
        "stationId": "06072",
        "parameterId": "temp_dry",
        #"limit": 10,
        "sortorder": "observed,DESC",
        "offset": 0
    }

    spac_url = "https://climate.spac.dk/api/records"
    spac_parameters = {

        #"from": "2026-02-27T09:32:45Z",
        "limit": "250"
    }



    async with httpx.AsyncClient(timeout=30, headers={"Authorization": f"Bearer {MY_TOKEN}"}) as client:
        pipeline = ETLPipeline(
            client=client,
            checkpoint=UrlCheckpointStore(),
            flush_every=2000
        )
        total = await pipeline.run(
            start_url=met_obs_url,
            base_params=met_obs_parameters
        )
        print(f"Ingestion completed: {total} rows downloaded to database")


asyncio.run(main())
