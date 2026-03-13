from models.sqlalchemy.stations import Station
from models.sqlalchemy.observations import Observation
from models.sqlalchemy.ballerup import BME280Reading, DS18B20Reading
from datetime import datetime
from typing import Optional

from uuid import UUID
from datetime import datetime


def parse_dt(value: Optional[str]) -> Optional[datetime]:
    if value is None:
        return None
    # Handle 'Z' (UTC) suffix
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def to_list(value):
    if isinstance(value, list):
        return value
    return [value]


def station_from_feature(feature):

    p = feature.properties
    lon, lat = feature.geometry.coordinates

    return Station(
        api_id=feature.id,
        name=p.name,
        owner=p.owner,
        country=p.country,
        station_id=p.stationId,
        wmo_station_id=p.wmoStationId,
        wmo_country_code=p.wmoCountryCode,
        region_id=p.regionId,
        type=p.type,
        status=p.status,
        station_height=p.stationHeight,
        barometer_height=p.barometerHeight,
        anemometer_height=p.anemometerHeight,
        parameter_ids=to_list(p.parameterId),
        operation_from=parse_dt(p.operationFrom),
        operation_to=parse_dt(p.operationTo),
        valid_from=parse_dt(p.validFrom),
        valid_to=parse_dt(p.validTo),
        created=parse_dt(p.created),
        updated=parse_dt(p.updated),
        longitude=lon,
        latitude=lat,
        raw_json=feature.model_dump(mode="json")
    )


def observation_from_feature(feature):

    p = feature.properties
    lon, lat = feature.geometry.coordinates

    return Observation(
        api_id=feature.id,
        station_id=p.stationId,
        parameter_id=p.parameterId,
        value=p.value,
        observed=parse_dt(p.observed),
        created=parse_dt(p.created),
        longitude=lon,
        latitude=lat,
        raw_json=feature.model_dump(mode="json")
    )




def record_to_observation_orm(rec: Record) -> Record:
    """
    Convert a Pydantic Record into the SQLAlchemy ORM RecordORM +
    child reading row (BME280 or DS18B20).
    """

    # Determine the reading type
    if hasattr(rec.reading, "BME280"):
        reading_type = "BME280"
        p = rec.reading.BME280

        orm = Record(
            id=str(rec.id),
            timestamp=parse_dt(rec.timestamp),
            reading_type="BME280",
            bme280=BME280Reading(
                temperature=p.temperature,
                pressure=p.pressure,
                humidity=p.humidity,
            )
        )
        return orm

    elif hasattr(rec.reading, "DS18B20"):
        reading_type = "DS18B20"
        p = rec.reading.DS18B20

        orm = Record(
            id=str(rec.id),
            timestamp=parse_dt(rec.timestamp),
            reading_type="DS18B20",
            ds18b20=DS18B20Reading(
                device_name=p.device_name,
                raw_reading=p.raw_reading,
            )
        )
        return orm

    else:
        raise ValueError(f"Unknown Reading type in record: {type(rec.reading)}")
