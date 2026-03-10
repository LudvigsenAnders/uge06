import httpx
import asyncio
import datetime
import pandas as pd
import matplotlib.pyplot as plt
import pandera.pandas as pa
import json
from ingestion.ingestor import ingest
from db.connection import close_asyncpg_pool, get_session, close_engine, stream, stream_batches, inspect_pool
from db.init_db import init_db
from db.db_utils import QueryRunner


async def main():
    await init_db()

    station_url = "https://opendataapi.dmi.dk/v2/metObs/collections/station/items"
    station_parameters = {
        #"datetime": "2018-02-12T00:00:00Z/2018-02-13T00:00:00Z",
        #"stationId": "06072",
        "limit": 2,
        #"offset": 0,
    }

    met_obs_url = "https://opendataapi.dmi.dk/v2/metObs/collections/observation/items"
    met_obs_parameters = {
        "datetime": "2018-02-12T00:00:00Z/2018-02-13T00:00:00Z",
        "stationId": "06072",
        "parameterId": "temp_dry",
        "limit": 2,
        "sortorder": "observed,DESC",
        "offset": 0
    }

    async with httpx.AsyncClient(timeout=30.0) as client:
        await ingest(client, station_url, station_parameters)
        print("Ingestion completed:")

            # # Get one row
            # one_row = await q.fetch_one(
            #     "SELECT * FROM orderdetails"
            # )
            # print(f"One row: {one_row}", "\n")

            # await inspect_pool(session)

            # query = (
            #     "select * from orders where employeeid in (2,5,8) "
            #     "and shipregion is not null and shipvia in (1,3) "
            #     "order by employeeid ASC, shipvia ASC;"
            # )
            # # Get rows
            # output = await q.fetch_all(
            #     query,
            #     as_mapping=True
            # )
            # print(f"Output: {output}", "\n")
            # print(f"Number of rows: {len(output)}", "\n")

            # # Streaming examples
            # async for row in stream("SELECT * FROM orderdetails LIMIT 10"):
            #     print(row)

            # async for batch in stream_batches("SELECT * FROM orderdetails", batch_size=1500):
            #     batch_df = pd.DataFrame(batch)
            #     print(batch_df.info())

            # await close_asyncpg_pool()

asyncio.run(main())
