"""
Pydantic models for data validation and serialization.

This module defines Pydantic BaseModel classes for validating and structuring data
from various sources including DMI (Danish Meteorological Institute) API responses
and Ballerup sensor readings.
"""

from pydantic import BaseModel, ConfigDict
from typing import List, Optional, Union
from uuid import UUID


# ============================================================
# Pydantic model for DMI data
# ============================================================
class Geometry(BaseModel):
    """Represents geographical geometry data from DMI API responses.

    Attributes:
        type: The geometry type (e.g., "Point").
        coordinates: List of coordinate values [longitude, latitude].
    """
    model_config = ConfigDict(extra="forbid")
    type: str
    coordinates: List[float]


class ObservationProperties(BaseModel):
    """Properties of a meteorological observation from DMI.

    Attributes:
        parameterId: The parameter being measured (e.g., "temp_dry").
        created: ISO timestamp when the observation was created.
        value: The measured numeric value.
        observed: ISO timestamp when the observation was made.
        stationId: Identifier of the weather station.
    """
    model_config = ConfigDict(extra="forbid")
    parameterId: str        # e.g. "temp_dry"
    created: str             # e.g. 2025-08-11T12:18:11.451095Z
    value: float             # numeric measurement
    observed: str            # 2018-02-12T00:00:00Z
    stationId: str           # e.g. "06072"


class StationProperties(BaseModel):
    """Properties of a weather station from DMI.

    Attributes:
        owner: Owner of the station.
        country: Country where the station is located.
        anemometerHeight: Height of the anemometer in meters.
        barometerHeight: Height of the barometer in meters.
        stationHeight: Height of the station in meters.
        wmoCountryCode: WMO country code.
        wmoStationId: WMO station identifier.
        stationId: Local station identifier.
        regionId: Regional identifier.
        name: Human-readable station name.
        type: Type of station.
        status: Operational status.
        parameterId: Parameter(s) measured by this station.
        operationFrom: Start date of operation.
        operationTo: End date of operation.
        validFrom: Start date of validity.
        validTo: End date of validity.
        created: Creation timestamp.
        updated: Last update timestamp.
    """
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
    """A GeoJSON Feature containing geometry and properties.

    Attributes:
        type: Always "Feature".
        id: Unique identifier for the feature.
        geometry: Geographical geometry data.
        properties: Either observation or station properties.
    """
    model_config = ConfigDict(extra="forbid")
    type: str
    id: UUID
    geometry: Geometry
    properties: PropertiesUnion


class Link(BaseModel):
    """A link object for API pagination and navigation.

    Attributes:
        href: The URL of the linked resource.
        rel: Relationship type (e.g., "next", "prev").
        type: MIME type of the linked resource.
        title: Human-readable title for the link.
    """
    model_config = ConfigDict(extra="forbid")
    href: str
    rel: str
    type: Optional[str] = None
    title: Optional[str] = None


class FeatureCollection(BaseModel):
    """A GeoJSON FeatureCollection containing multiple features.

    Attributes:
        type: Always "FeatureCollection".
        features: List of Feature objects.
        timeStamp: Timestamp when the collection was generated.
        numberReturned: Number of features returned.
        links: List of navigation links.
    """
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
    """Represents a UUID bearer token for API authentication.

    Attributes:
        token: The UUID token used for bearer authentication.
    """
    token: UUID


# ============================================================
# Sensor reading models
# ============================================================
class BME280(BaseModel):
    """Sensor readings from a BME280 environmental sensor.

    Attributes:
        temperature: Temperature in degrees Celsius.
        pressure: Atmospheric pressure in hPa.
        humidity: Relative humidity as percentage.
    """
    model_config = ConfigDict(extra="forbid")
    temperature: float
    pressure: float
    humidity: float


class DS18B20(BaseModel):
    """Sensor readings from a DS18B20 temperature sensor.

    Attributes:
        device_name: Unique identifier/name of the sensor device.
        raw_reading: Raw temperature reading as integer value.
    """
    model_config = ConfigDict(extra="forbid")
    device_name: str  # = Field(min_length=12, max_length=12)
    raw_reading: int


# ============================================================
# Wrappers (externally tagged union)
# ============================================================
class BME280Wrapper(BaseModel):
    """Wrapper for BME280 sensor readings in externally tagged union.

    Attributes:
        BME280: The BME280 sensor data.
    """
    model_config = ConfigDict(extra="forbid")
    BME280: BME280


class DS18B20Wrapper(BaseModel):
    """Wrapper for DS18B20 sensor readings in externally tagged union.

    Attributes:
        DS18B20: The DS18B20 sensor data.
    """
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
    """A complete sensor reading record with metadata.

    Attributes:
        id: Unique identifier for the record.
        timestamp: ISO timestamp when the reading was taken.
        reading: The sensor reading data (BME280 or DS18B20).
    """
    model_config = ConfigDict(extra="forbid")

    id: UUID
    timestamp: str
    reading: Reading


# ============================================================
# Response Models
# ============================================================
class RecordResponse(BaseModel):
    """API response containing a single sensor record.

    Attributes:
        record: The sensor record data.
    """
    model_config = ConfigDict(extra="forbid")
    record: Record


class RecordsResponse(BaseModel):
    """API response containing multiple sensor records.

    Attributes:
        records: List of sensor record data.
    """
    model_config = ConfigDict(extra="forbid")
    records: List[Record]
