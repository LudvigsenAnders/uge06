import httpx
import asyncio
import datetime
import pandas as pd
import matplotlib.pyplot as plt
import pandera.pandas as pa
import json


async def main():
    async with httpx.AsyncClient() as client:

        try:
            response = await client.get("https://opendataapi.dmi.dk/v2/metObs/collections/observation/items?datetime=2026-03-01T13%3A00%3A00Z&stationId=06072")
            response.raise_for_status()
        except httpx.TimeoutException:
            print("Request timed out!")
        except httpx.HTTPStatusError as exc:
            print(f"HTTP error {exc.response.status_code}: {exc}")

        data = response.json()
        print(data)
        pretty = json.dumps(data, indent=4)
        print(pretty)

        df = pd.json_normalize(data["features"])

        print(df.info())
        print(df.head(20))

asyncio.run(main())
