
from sqlalchemy import Column, String, Float, JSON, TIMESTAMP
from sqlalchemy.dialects.postgresql import UUID
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
