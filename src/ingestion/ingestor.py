from .data_request import request_data
from models.pydantic_model import FeatureCollection, StationProperties, ObservationProperties
from .mapper import station_from_feature, observation_from_feature
from db.connection import get_session, close_engine, inspect_pool
from db.db_utils import QueryRunner
import httpx
from ingestion.repository import save_stations, save_observations


async def ingest(client: httpx.AsyncClient, url: str, parameters: dict):
    # Step 1: fetch JSON
    raw = await request_data(client, url, parameters)
    print("Raw data fetched:", raw)

    # Step 2: parse unified FeatureCollection
    data = FeatureCollection.model_validate(raw)
    print(f"Parsed {len(data.features)} features from the response.")

    # Prepare lists for batch save
    station_rows = []
    observation_rows = []

    for f in data.features:
        props = f.properties

        if isinstance(props, StationProperties):
            station_rows.append(station_from_feature(f))

        elif isinstance(props, ObservationProperties):
            observation_rows.append(observation_from_feature(f))

        else:
            raise ValueError(f"Unknown feature properties type: {props}")

    # Step 3: store objects (batched)
    async for session in get_session():
        q = QueryRunner(session)
        async with q.transaction():
            now = await q.fetch_value("SELECT NOW()")
            print(f"Now: {now}\n")

            # Save stations (BATCHED UPSERT with ON CONFLICT DO UPDATE)
            if station_rows:
                print(f"Saving {len(station_rows)} stations...")
                await save_stations(session, station_rows)

            # Save observations (BATCHED INSERT with ON CONFLICT DO NOTHING)
            if observation_rows:
                print(f"Saving {len(observation_rows)} observations...")
                await save_observations(session, observation_rows)

    await close_engine()
    await inspect_pool(session)
