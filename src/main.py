import httpx
import asyncio
import datetime
import pandas as pd
import matplotlib.pyplot as plt
import pandera.pandas as pa
import json
from pydantic import BaseModel, Field
from typing import List, Optional, Union
from uuid import UUID

async def request_data(client, url, parameters):
    try:
        response = await client.get(url, params=parameters)
        response.raise_for_status()
        return response.json()
    except httpx.TimeoutException:
        print("Request timed out!")
    except httpx.HTTPStatusError as exc:
        print(f"HTTP error {exc.response.status_code}: {exc}")
    return None


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







async def main():
    async with httpx.AsyncClient() as client:
        station_url = "https://opendataapi.dmi.dk/v2/metObs/collections/station/items"
        station_parameters = {
            #"datetime": "2018-02-12T00:00:00Z/2018-02-13T00:00:00Z",
            #"stationId": "06072",
            "limit": 2,
            #"offset": 0,
        }

        met_obs_url = "https://opendataapi.dmi.dk/v2/metObs/collections/observation/items"
        met_obs_parameters = {
            "datetime": "2018-02-12T00:00:00Z/2018-02-13T00:00:00Z",
            "stationId": "06072",
            "parameterId": "temp_dry",
            "limit": 2,
            "sortorder": "observed,DESC",
            "offset": 0
        }





        data_station = await request_data(client, station_url, station_parameters)
        fc = FeatureCollection.model_validate(data_station)

        for f in fc.features:
            if isinstance(f.properties, StationProperties):
                print("Station:", f.properties.name)
            else:
                print("Observation:", f.properties.parameterId)
        
        
        
        
        df_station = pd.json_normalize(data_station["features"])

        #print(f"Station Data: {data_station}")  
        #print(df_station.info())
        #print(df_station)

        data_met_obs = await request_data(client, met_obs_url, met_obs_parameters)
        fc = FeatureCollection.model_validate(data_met_obs)
        for f in fc.features:
            if isinstance(f.properties, StationProperties):
                print("Station:", f.properties.name)
            else:
                print("Observation:", f.properties.parameterId)

        df_met_obs = pd.json_normalize(data_met_obs["features"])


        #print(f"Met Observations Data: {data_met_obs}")
        #print(df_met_obs.info())
        #print(df_met_obs)

asyncio.run(main())
