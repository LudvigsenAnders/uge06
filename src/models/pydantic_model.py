from pydantic import BaseModel, ConfigDict
from typing import List, Optional, Union
from uuid import UUID


# ============================================================
# Pydantic model for DMI data
# ============================================================
class Geometry(BaseModel):
    model_config = ConfigDict(extra="forbid")
    type: str
    coordinates: List[float]


class ObservationProperties(BaseModel):
    model_config = ConfigDict(extra="forbid")
    parameterId: str        # e.g. "temp_dry"
    created: str             # e.g. 2025-08-11T12:18:11.451095Z
    value: float             # numeric measurement
    observed: str            # 2018-02-12T00:00:00Z
    stationId: str           # e.g. "06072"


class StationProperties(BaseModel):
    model_config = ConfigDict(extra="forbid")
    owner: Optional[str] = None
    country: Optional[str] = None
    anemometerHeight: Optional[float] = None
    barometerHeight: Optional[float] = None
    stationHeight: Optional[float] = None
    wmoCountryCode: Optional[str] = None
    wmoStationId: Optional[str] = None
    stationId: str
    regionId: Optional[str] = None
    name: Optional[str] = None
    type: Optional[str] = None
    status: Optional[str] = None
    parameterId: str | list[str]
    operationFrom: Optional[str] = None
    operationTo: Optional[str] = None
    validFrom: Optional[str] = None
    validTo: Optional[str] = None
    created: Optional[str] = None
    updated: Optional[str] = None


PropertiesUnion = Union[ObservationProperties, StationProperties]


class Feature(BaseModel):
    model_config = ConfigDict(extra="forbid")
    type: str
    id: UUID
    geometry: Geometry
    properties: PropertiesUnion


class Link(BaseModel):
    model_config = ConfigDict(extra="forbid")
    href: str
    rel: str
    type: Optional[str] = None
    title: Optional[str] = None


class FeatureCollection(BaseModel):
    model_config = ConfigDict(extra="forbid")
    type: str
    features: List[Feature]
    timeStamp: str
    numberReturned: int
    links: List[Link]


# ============================================================
# Pydantic model for Ballerup data
# ============================================================

# ============================================================
# Security
# ============================================================
class BearerAuth(BaseModel):
    """Represents a UUID bearer token."""
    token: UUID


# ============================================================
# Sensor reading models
# ============================================================
class BME280(BaseModel):
    model_config = ConfigDict(extra="forbid")
    temperature: float
    pressure: float
    humidity: float


class DS18B20(BaseModel):
    model_config = ConfigDict(extra="forbid")
    device_name: str  # = Field(min_length=12, max_length=12)
    raw_reading: int


# ============================================================
# Wrappers (externally tagged union)
# ============================================================
class BME280Wrapper(BaseModel):
    model_config = ConfigDict(extra="forbid")
    BME280: BME280


class DS18B20Wrapper(BaseModel):
    model_config = ConfigDict(extra="forbid")
    DS18B20: DS18B20


# ------------------------------------------------------------
# Externally tagged union: exactly one of {BME280: {...}} or {DS18B20: {...}}
# ------------------------------------------------------------
Reading = Union[BME280Wrapper, DS18B20Wrapper]


# ============================================================
# Record
# ============================================================
class Record(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: UUID
    timestamp: str
    reading: Reading


# ============================================================
# Response Models
# ============================================================
class RecordResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")
    record: Record


class RecordsResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")
    records: List[Record]
