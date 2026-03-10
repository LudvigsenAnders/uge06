from .data_request import request_data
from models.pydantic_model import FeatureCollection, StationProperties, ObservationProperties
from .mapper import station_from_feature, observation_from_feature
from db.connection import get_session, close_engine, inspect_pool
from db.db_utils import QueryRunner
import httpx


async def ingest(client: httpx.AsyncClient, url: str, parameters: dict):
    # Step 1: fetch JSON
    raw = await request_data(client, url, parameters)
    print("Raw data fetched:", raw)
    # Step 2: parse unified FeatureCollection
    data = FeatureCollection.model_validate(raw)
    print(f"Parsed {len(data.features)} features from the response.")
    print(f"Feature IDs: {[f.id for f in data.features]}")
    # Step 3: store objects
    async for session in get_session():
        q = QueryRunner(session)
        async with q.transaction():
            # Get a single scalar
            now = await q.fetch_value("SELECT NOW()")
            print(f"Now: {now}", "\n")

            for f in data.features:

                # Identify which "properties" type this feature has
                props = f.properties

                if isinstance(props, StationProperties):
                    obj = station_from_feature(f)

                elif isinstance(props, ObservationProperties):
                    obj = observation_from_feature(f)

                else:
                    raise ValueError(f"Unknown feature properties type: {props}")
                print(f"Storing object with ID {obj.id} in the database...")
                session.add(obj)

            await session.commit()
    await close_engine()
    await inspect_pool(session)
