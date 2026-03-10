import httpx
import asyncio
from ingestion.ingestor import ingest
from db.init_db import init_db

# import pandas as pd
# import matplotlib.pyplot as plt
# import pandera.pandas as pa
# import json


async def main():
    await init_db()

    station_url = "https://opendataapi.dmi.dk/v2/metObs/collections/station/items"
    station_parameters = {
        #"datetime": "2018-02-12T00:00:00Z/2018-02-13T00:00:00Z",
        #"stationId": "06072",
        "limit": 10,
        #"offset": 0,
    }

    met_obs_url = "https://opendataapi.dmi.dk/v2/metObs/collections/observation/items"
    met_obs_parameters = {
        "datetime": "2018-02-12T00:00:00Z/2018-02-13T00:00:00Z",
        "stationId": "06072",
        "parameterId": "temp_dry",
        "limit": 10,
        "sortorder": "observed,DESC",
        "offset": 0
    }

    
    async with httpx.AsyncClient(timeout=30.0) as client:
        await ingest(client, station_url, station_parameters)
        await ingest(client, met_obs_url, met_obs_parameters)
        print("Ingestion completed:")

            # # Streaming examples
            # async for row in stream("SELECT * FROM orderdetails LIMIT 10"):
            #     print(row)

            # async for batch in stream_batches("SELECT * FROM orderdetails", batch_size=1500):
            #     batch_df = pd.DataFrame(batch)
            #     print(batch_df.info())

            # await close_asyncpg_pool()

asyncio.run(main())
