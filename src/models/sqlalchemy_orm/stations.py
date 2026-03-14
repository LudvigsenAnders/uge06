

from typing import Optional, List
from datetime import datetime
import uuid
from sqlalchemy import String, Float, TIMESTAMP, UniqueConstraint, BigInteger, Identity
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.dialects.postgresql import JSONB as PG_JSONB
from sqlalchemy.orm import Mapped, mapped_column
from .base import Base


class Station(Base):
    __tablename__ = "stations"

    __table_args__ = (
        UniqueConstraint("api_id", name="uq_stations_api_id"),
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(start=1), primary_key=True)

    api_id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), nullable=False)
    name: Mapped[Optional[str]] = mapped_column(String(255), index=True, nullable=True)
    owner: Mapped[Optional[str]] = mapped_column(String(255), index=True, nullable=True)
    country: Mapped[Optional[str]] = mapped_column(String(255), index=True, nullable=True)

    station_id: Mapped[str] = mapped_column(String(255), index=True, nullable=False)
    wmo_station_id: Mapped[Optional[str]] = mapped_column(String(255), index=True, nullable=True)
    wmo_country_code: Mapped[Optional[str]] = mapped_column(String(255), index=True, nullable=True)
    region_id: Mapped[Optional[str]] = mapped_column(String(255), index=True, nullable=True)

    type: Mapped[Optional[str]] = mapped_column(String(255), index=True, nullable=True)
    status: Mapped[Optional[str]] = mapped_column(String(255), index=True, nullable=True)

    station_height: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    barometer_height: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    anemometer_height: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    parameter_ids: Mapped[Optional[List[str]]] = mapped_column(ARRAY(String(255)), nullable=True)

    operation_from: Mapped[Optional[datetime]] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    operation_to: Mapped[Optional[datetime]] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    valid_from: Mapped[Optional[datetime]] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    valid_to: Mapped[Optional[datetime]] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    created: Mapped[Optional[datetime]] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    updated: Mapped[Optional[datetime]] = mapped_column(TIMESTAMP(timezone=True), nullable=True)

    latitude: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    longitude: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    raw_json: Mapped[Optional[dict]] = mapped_column(PG_JSONB, nullable=True)
