
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
    name = Column(String(255))
    owner = Column(String(255))
    country = Column(String(255))

    station_id = Column(String(255))
    wmo_station_id = Column(String(255))
    wmo_country_code = Column(String(255))
    region_id = Column(String(255))

    type = Column(String(255))
    status = Column(String(255))

    station_height = Column(Float)
    barometer_height = Column(Float)
    anemometer_height = Column(Float)

    parameter_ids = Column(ARRAY(String(255)))

    operation_from = Column(TIMESTAMP(timezone=True))
    operation_to = Column(TIMESTAMP(timezone=True))
    valid_from = Column(TIMESTAMP(timezone=True))
    valid_to = Column(TIMESTAMP(timezone=True))
    created = Column(TIMESTAMP(timezone=True))
    updated = Column(TIMESTAMP(timezone=True))

    latitude = Column(Float)
    longitude = Column(Float)

    raw_json = Column(JSON)
