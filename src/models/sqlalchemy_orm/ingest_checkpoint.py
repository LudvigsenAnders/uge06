from typing import Optional
from datetime import datetime
from sqlalchemy import String, TIMESTAMP
from sqlalchemy import UniqueConstraint, BigInteger, Identity
from sqlalchemy.orm import Mapped, mapped_column
from .base import Base


class IngestCheckpoint(Base):
    __tablename__ = "ingest_checkpoint"

    __table_args__ = (
        UniqueConstraint("id", name="uq_ingest_checkpoint_id"),
    )

    id: Mapped[int] = mapped_column(BigInteger, Identity(start=1), primary_key=True)

    next_url: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    table_name: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    created: Mapped[Optional[datetime]] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
