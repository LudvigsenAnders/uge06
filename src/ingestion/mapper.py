from models.sqlalchemy_models import Station, Observation


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
        operation_from=p.operationFrom,
        operation_to=p.operationTo,
        valid_from=p.validFrom,
        valid_to=p.validTo,
        created=p.created,
        updated=p.updated,
        longitude=lon,
        latitude=lat,
        raw_json=feature.model_dump()
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
        observed=p.observed,
        created=p.created,
        longitude=lon,
        latitude=lat,
        raw_json=feature.model_dump()
    )
