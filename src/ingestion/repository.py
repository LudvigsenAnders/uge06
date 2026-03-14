
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Iterable

from models.sqlalchemy_orm.observations import Observation
from models.sqlalchemy_orm.stations import Station

# Adjust this to tune performance
BATCH_SIZE = 500


async def _chunked(iterable: Iterable, size: int):
    """Yield items in batches of size N."""
    batch = []
    for item in iterable:
        batch.append(item)
        if len(batch) == size:
            yield batch
            batch = []
    if batch:
        yield batch


# -------------------------------------------------------------------
# SAVE OBSERVATIONS (WITH BATCHING)
# -------------------------------------------------------------------
async def save_observations(session: AsyncSession, obs_rows: list[Observation]):
    """
    Insert observations into PostgreSQL using ON CONFLICT DO NOTHING
    in batches to achieve high throughput.

    Requires UNIQUE constraint:
        UNIQUE (station_id, parameter_id, observed)
    """
    if not obs_rows:
        return

    async for batch in _chunked(obs_rows, BATCH_SIZE):
        # Convert ORM objects → plain dicts
        values = [
            {
                "api_id": row.api_id,
                "station_id": row.station_id,
                "parameter_id": row.parameter_id,
                "value": row.value,
                "observed": row.observed,     # datetime
                "created": row.created,       # datetime
                "latitude": row.latitude,
                "longitude": row.longitude,
                "raw_json": row.raw_json,     # already JSON-safe
            }
            for row in batch
        ]

        stmt = (
            insert(Observation)
            .values(values)
            .on_conflict_do_nothing(
                # Use your unique constraint
                index_elements=["station_id", "parameter_id", "observed"]
            )
        )

        await session.execute(stmt)

    await session.commit()


# -------------------------------------------------------------------
# SAVE STATIONS (WITH BATCHING)
# -------------------------------------------------------------------
async def save_stations(session: AsyncSession, station_rows: list[Station]):
    """
    UPSERT stations into PostgreSQL using ON CONFLICT DO UPDATE.
    Updates fields when a station already exists (same primary key 'id').

    Requires primary key on stations.id
    """
    if not station_rows:
        return

    async for batch in _chunked(station_rows, BATCH_SIZE):
        values = [
            {
                "api_id": row.api_id,
                "name": row.name,
                "owner": row.owner,
                "country": row.country,
                "station_id": row.station_id,
                "wmo_station_id": row.wmo_station_id,
                "wmo_country_code": row.wmo_country_code,
                "region_id": row.region_id,
                "type": row.type,
                "status": row.status,
                "station_height": row.station_height,
                "barometer_height": row.barometer_height,
                "anemometer_height": row.anemometer_height,
                "parameter_ids": row.parameter_ids,
                "operation_from": row.operation_from,
                "operation_to": row.operation_to,
                "valid_from": row.valid_from,
                "valid_to": row.valid_to,
                "created": row.created,
                "updated": row.updated,
                "latitude": row.latitude,
                "longitude": row.longitude,
                "raw_json": row.raw_json,
            }
            for row in batch
        ]

        stmt = insert(Station).values(values)

        update_cols = {
            # Only fields where updates make sense
            "api_id": stmt.excluded.api_id,
            "name": stmt.excluded.name,
            "owner": stmt.excluded.owner,
            "country": stmt.excluded.country,
            "station_id": stmt.excluded.station_id,
            "wmo_station_id": stmt.excluded.wmo_station_id,
            "wmo_country_code": stmt.excluded.wmo_country_code,
            "region_id": stmt.excluded.region_id,
            "type": stmt.excluded.type,
            "status": stmt.excluded.status,

            "station_height": stmt.excluded.station_height,
            "barometer_height": stmt.excluded.barometer_height,
            "anemometer_height": stmt.excluded.anemometer_height,

            "parameter_ids": stmt.excluded.parameter_ids,

            "operation_from": stmt.excluded.operation_from,
            "operation_to": stmt.excluded.operation_to,
            "valid_from": stmt.excluded.valid_from,
            "valid_to": stmt.excluded.valid_to,
            "created": stmt.excluded.created,
            "updated": stmt.excluded.updated,

            "latitude": stmt.excluded.latitude,
            "longitude": stmt.excluded.longitude,

            "raw_json": stmt.excluded.raw_json,
        }

        stmt = stmt.on_conflict_do_update(
            index_elements=["api_id"],  # Primary key
            set_=update_cols
        )

        await session.execute(stmt)

    await session.commit()
