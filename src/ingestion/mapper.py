from models.sqlalchemy_orm.stations import Station
from models.sqlalchemy_orm.observations import Observation
from models.pydantic_model import Feature, Record
from datetime import datetime
from typing import Optional


def parse_dt(value: Optional[str]) -> Optional[datetime]:
    if value is None:
        return None
    # Handle 'Z' (UTC) suffix
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def to_list(value):
    if isinstance(value, list):
        return value
    return [value]


def station_from_feature_to_orm(feature: Feature) -> Station:

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


def observation_from_feature_to_orm(feature: Feature) -> Observation:

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


def observations_from_bme280_to_ORM(rec: Record) -> list[Observation]:
    r = rec.reading.BME280
    ts = rec.timestamp
    base = dict(
        api_id=str(rec.id),
        station_id="ballerup-BME280",
        observed=parse_dt(ts),
        created=parse_dt(ts),
        latitude=None,
        longitude=None,
        raw_json=rec.model_dump(mode="json"),
    )
    return [
        Observation(parameter_id="temperature", value=r.temperature, **base),
        Observation(parameter_id="pressure", value=r.pressure, **base),
        Observation(parameter_id="humidity", value=r.humidity, **base),
    ]


def observations_from_DS18B20_to_ORM(rec: Record) -> list[Observation]:
    r = rec.reading.DS18B20
    ts = rec.timestamp
    base = dict(
        api_id=str(rec.id),
        station_id="ballerup-DS18B20-" + r.device_name,
        observed=parse_dt(ts),
        created=parse_dt(ts),
        latitude=None,
        longitude=None,
        raw_json=rec.model_dump(mode="json"),
    )
    return Observation(parameter_id="raw_reading", value=r.raw_reading, **base)
