from __future__ import annotations
from sqlalchemy import (String,
                        Integer,
                        Float,
                        CheckConstraint,
                        BigInteger,
                        Identity,
                        JSON,
                        Enum,
                        Index,
                        DateTime,
                        ForeignKey
                        )
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship, Mapped, mapped_column
from datetime import datetime
from typing import Optional, Literal
from .base import Base


# --- Discriminator type for union ---
ReadingType = Literal["BME280", "DS18B20"]


class RecordORM(Base):
    __tablename__ = "records"

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    reading_type: Mapped[ReadingType] = mapped_column(
        Enum("BME280", "DS18B20", name="reading_type"),
        nullable=False,
    )

    # NOTE: Target type is available via annotation because of __future__ import
    bme280: Mapped[Optional[BME280ReadingORM]] = relationship(
        back_populates="record",
        cascade="all, delete-orphan",
        uselist=False,
        lazy="joined",
    )
    ds18b20: Mapped[Optional[DS18B20ReadingORM]] = relationship(
        back_populates="record",
        cascade="all, delete-orphan",
        uselist=False,
        lazy="joined",
    )

    __table_args__ = (
        Index("ix_records_timestamp", "timestamp"),
    )


class BME280ReadingORM(Base):
    __tablename__ = "bme280_readings"

    record_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("records.id", ondelete="CASCADE"),
        primary_key=True,
    )
    temperature: Mapped[float] = mapped_column(Float, nullable=False)
    pressure: Mapped[float] = mapped_column(Float, nullable=False)
    humidity: Mapped[float] = mapped_column(Float, nullable=False)

    record: Mapped[RecordORM] = relationship(back_populates="bme280")

    __table_args__ = (
        CheckConstraint("humidity >= 0.0 AND humidity <= 100.0", name="ck_bme280_humidity_range"),
    )


class DS18B20ReadingORM(Base):
    __tablename__ = "ds18b20_readings"

    record_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("records.id", ondelete="CASCADE"),
        primary_key=True,
    )
    device_name: Mapped[str] = mapped_column(String(12), nullable=False)
    raw_reading: Mapped[int] = mapped_column(Integer, nullable=False)

    record: Mapped[RecordORM] = relationship(back_populates="ds18b20")

    __table_args__ = (
        CheckConstraint("length(device_name) = 12", name="ck_ds18b20_device_len"),
    )
