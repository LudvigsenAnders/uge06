from pydantic import BaseModel
from typing import List, Optional, Union
from uuid import UUID


class Geometry(BaseModel):
    type: str
    coordinates: List[float]


class ObservationProperties(BaseModel):
    parameterId: str         # e.g. "temp_dry"
    created: str             # e.g. 2025-08-11T12:18:11.451095Z
    value: float             # numeric measurement
    observed: str            # 2018-02-12T00:00:00Z
    stationId: str           # e.g. "06072"


class StationProperties(BaseModel):
    owner: str
    country: str
    anemometerHeight: Optional[float] = None
    barometerHeight: Optional[float] = None
    stationHeight: Optional[float] = None
    wmoCountryCode: str
    wmoStationId: str
    stationId: str
    regionId: str
    name: str
    type: str
    status: str
    parameterId: List[str]
    operationFrom: Optional[str] = None
    operationTo: Optional[str] = None
    validFrom: Optional[str] = None
    validTo: Optional[str] = None
    created: Optional[str] = None
    updated: Optional[str] = None


PropertiesUnion = Union[ObservationProperties, StationProperties]


class Feature(BaseModel):
    type: str
    id: UUID
    geometry: Geometry
    properties: PropertiesUnion


class Link(BaseModel):
    href: str
    rel: str
    type: Optional[str] = None
    title: Optional[str] = None


class FeatureCollection(BaseModel):
    type: str
    features: List[Feature]
    timeStamp: str
    numberReturned: int
    links: List[Link]
