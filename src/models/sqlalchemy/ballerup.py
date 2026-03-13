from sqlalchemy import Column, String, Integer, Float, CheckConstraint, BigInteger, Identity, JSON
from sqlalchemy.dialects.postgresql import UUID
from .base import Base

# ============================================================
# BME280 reading (one-to-one with records)
# ============================================================


class BME280Reading(Base):
    __tablename__ = "bme280_readings"

    id = Column(BigInteger, Identity(start=1), primary_key=True)
    api_id = Column(UUID)
    temperature = Column(Float, nullable=False)
    pressure = Column(Float, nullable=False)
    humidity = Column(Float, nullable=False)
    raw_json = Column(JSON)

    __table_args__ = (
        CheckConstraint("humidity >= 0.0 AND humidity <= 100.0", name="ck_bme280_humidity_range"),
    )


# ============================================================
# DS18B20 reading (one-to-one with records)
# ============================================================

class DS18B20Reading(Base):
    __tablename__ = "ds18b20_readings"

    id = Column(BigInteger, Identity(start=1), primary_key=True)
    api_id = Column(UUID)
    device_name = Column(String(12), nullable=False)
    raw_reading = Column(Integer, nullable=False)
    raw_json = Column(JSON)



