from src.ingestion.data_request import request_data
from src.models.pydantic_model import FeatureCollection, StationProperties, ObservationProperties
from src.ingestion.mapper import station_from_feature, observation_from_feature
from src.db.connection import get_session
import httpx


async def ingest(client: httpx.AsyncClient, url: str, parameters: dict):
    # Step 1: fetch JSON
    raw = await request_data(client, url, parameters)

    # Step 2: parse unified FeatureCollection
    data = FeatureCollection.model_validate(raw)

    # Step 3: store objects
    async with get_session() as session:
        for f in data.features:

            # Identify which "properties" type this feature has
            props = f.properties

            if isinstance(props, StationProperties):
                obj = station_from_feature(f)

            elif isinstance(props, ObservationProperties):
                obj = observation_from_feature(f)

            else:
                raise ValueError(f"Unknown feature properties type: {props}")

            session.add(obj)

        await session.commit()
