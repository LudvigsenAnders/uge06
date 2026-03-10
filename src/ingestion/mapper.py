from models.sqlalchemy.stations import Station
from models.sqlalchemy.observations import Observation
from datetime import datetime
from typing import Optional


def parse_dt(value: Optional[str]) -> Optional[datetime]:
    if value is None:
        return None
    # Handle 'Z' (UTC) suffix
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def station_from_feature(feature):

    print("imported mapper module")

    p = feature.properties
    lon, lat = feature.geometry.coordinates

    return Station(
        id=feature.id,
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
        parameter_ids=p.parameterId,
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

    print("imported mapper module")
    p = feature.properties
    lon, lat = feature.geometry.coordinates

    return Observation(
        id=feature.id,
        station_id=p.stationId,
        parameter_id=p.parameterId,
        value=p.value,
        observed=parse_dt(p.observed),
        created=parse_dt(p.created),
        longitude=lon,
        latitude=lat,
        raw_json=feature.model_dump(mode="json")
    )
