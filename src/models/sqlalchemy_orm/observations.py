
from typing import Optional
from datetime import datetime
import uuid
from sqlalchemy import String, Float, TIMESTAMP, UniqueConstraint, BigInteger, Identity
from sqlalchemy.orm import Mapped, mapped_column
from .base import Base

from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.dialects.postgresql import JSONB as PG_JSONB


# If using Postgres, prefer JSONB and native UUID.
# If using SQLite, JSONB/UUID won't be available; use to String/JSON.


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

    id: Mapped[int] = mapped_column(BigInteger, Identity(start=1), primary_key=True)

    api_id: Mapped[uuid.UUID] = mapped_column(PG_UUID(as_uuid=True), nullable=False)
    station_id: Mapped[str] = mapped_column(String(255), index=True, nullable=False)
    parameter_id: Mapped[str] = mapped_column(String(255), nullable=False)

    value: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    observed: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)
    created: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)

    latitude: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    longitude: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    raw_json: Mapped[Optional[dict]] = mapped_column(PG_JSONB, nullable=True)
