
from sqlalchemy import Column, String, Float, JSON, TIMESTAMP, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID, ARRAY
from sqlalchemy import BigInteger, Identity
from .base import Base


class Station(Base):
    __tablename__ = "stations"

    __table_args__ = (
        UniqueConstraint("api_id", name="uq_stations_api_id"),
    )

    id = Column(BigInteger, Identity(start=1), primary_key=True)

    api_id = Column(UUID)
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
