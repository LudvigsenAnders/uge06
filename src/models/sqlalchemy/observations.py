
from sqlalchemy import Column, String, Float, JSON, TIMESTAMP
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy import UniqueConstraint, BigInteger, Identity
from .base import Base


class Observation(Base):
    __tablename__ = "observations"

    __table_args__ = (
        UniqueConstraint(
            "station_id",
            "parameter_id",
            "observed",
            name="uq_observations_station_id_parameter_id_observed",
        ),
    )

    id = Column(BigInteger, Identity(start=1), primary_key=True)

    api_id = Column(UUID)
    station_id = Column(String(255), index=True)

    parameter_id = Column(String(255))
    value = Column(Float)

    observed = Column(TIMESTAMP(timezone=True))
    created = Column(TIMESTAMP(timezone=True))

    latitude = Column(Float)
    longitude = Column(Float)

    raw_json = Column(JSON)
