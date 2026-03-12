from helper_functions.helper_functions import setup_logging
import httpx
import asyncio
from ingestion.ingestor import ingest_streaming
from db.init_db import init_db


# import pandas as pd
# import matplotlib.pyplot as plt
# import pandera.pandas as pa
# import json


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
        #"datetime": "2018-02-12T00:00:00Z/2018-02-13T00:00:00Z",
        "stationId": "06072",
        "parameterId": "temp_dry",
        #"limit": 10,
        "sortorder": "observed,DESC",
        "offset": 0
    }

    async with httpx.AsyncClient(timeout=30.0) as client:
        await ingest_streaming(client, station_url, station_parameters)
        await ingest_streaming(client, met_obs_url, met_obs_parameters)
        print("Ingestion completed:")

asyncio.run(main())
