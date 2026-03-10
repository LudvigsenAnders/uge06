
from sqlalchemy import Column, String, Float, JSON, TIMESTAMP
from sqlalchemy.dialects.postgresql import UUID, ARRAY
from sqlalchemy.orm import declarative_base


Base = declarative_base()


class Observation(Base):
    __tablename__ = "observations"

    id = Column(UUID, primary_key=True)
    station_id = Column(String, index=True)

    parameter_id = Column(String)
    value = Column(Float)

    observed = Column(TIMESTAMP(timezone=True))
    created = Column(TIMESTAMP(timezone=True))

    latitude = Column(Float)
    longitude = Column(Float)

    raw_json = Column(JSON)


class Station(Base):
    __tablename__ = "stations"

    id = Column(UUID, primary_key=True)
    name = Column(String)
    owner = Column(String)
    country = Column(String)

    station_id = Column(String)
    wmo_station_id = Column(String)
    wmo_country_code = Column(String)
    region_id = Column(String)

    type = Column(String)
    status = Column(String)

    station_height = Column(Float)
    barometer_height = Column(Float)
    anemometer_height = Column(Float)

    parameter_ids = Column(ARRAY(String))

    operation_from = Column(TIMESTAMP(timezone=True))
    operation_to = Column(TIMESTAMP(timezone=True))
    valid_from = Column(TIMESTAMP(timezone=True))
    valid_to = Column(TIMESTAMP(timezone=True))
    created = Column(TIMESTAMP(timezone=True))
    updated = Column(TIMESTAMP(timezone=True))

    latitude = Column(Float)
    longitude = Column(Float)

    raw_json = Column(JSON)
