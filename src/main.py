from helper_functions.helper_functions import setup_logging
import httpx
import asyncio
#rom ingestion.ingestor import ingest_streaming
from db.init_db import init_db


# import pandas as pd
# import matplotlib.pyplot as plt
# import pandera.pandas as pa
# import json


LOGGING = True
MAX_CONCURRENCY = 5


# somewhere in your app
from ingestion.ingestor import StreamingIngestor
from ingestion.checkpoint_store import UrlCheckpointStore






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
        "limit": "250",
        #"Authorization": "Bearer"
    }

    # async with httpx.AsyncClient(timeout=30.0) as client:
    #     await ingest_streaming(client, station_url, station_parameters)
    #     #await ingest_streaming(client, met_obs_url, met_obs_parameters)
    #     print("Ingestion completed:")
    token = "4t4b6sUUR4sTMvVHX-GM2AoGKhe7YnNdQXKcO2XccCs"

    async with httpx.AsyncClient(timeout=30, headers={"Authorization": f"Bearer {token}"}) as client:
        ingestor = StreamingIngestor(
            client=client,
            checkpoint=UrlCheckpointStore(),
            flush_every=2000
        )
        total = await ingestor.run(
            start_url=spac_url,
            base_params=spac_parameters
        )
        print(f"Ingestion completed: {total} rows downloaded to database")


asyncio.run(main())
