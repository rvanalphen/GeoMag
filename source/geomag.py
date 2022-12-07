from pydantic import BaseModel, validator, ValidationError
from pathlib import Path
from typing import List, Optional
import pydantic
from pyproj import CRS
import string

class GeoMag(BaseModel):

    filepath: Path or string
    input_epsg: Optional[str] = None
    output_epsg: Optional[str] = None
    dates: Optional[List[str]] = None
    elevation: Optional[str] = '0'

    @validator('filepath',pre=True,check_fields=True)
    def path_validator(cls, f) -> Path:
        if not f.is_file():
            raise AttributeError(
                'The file specified does not exist; check filepath')
        return f

    @validator('input_epsg','output_epsg')
    def epsg_validator(cls, code) -> str:
        if not code.isdigit():
            print("WOW")
            raise ValueError(
                "EPSG: code must be a string of all numbers, example: '4326' ")
        
        return code

    @validator('dates')
    def dates_validator(cls,dts):
        if not type(dts) == list:
            raise TypeError('Dates must be of type List')
                
        return dts

    @validator('elevation')
    def elevation_validator(cls, elev) -> str:
        if not elev.isdigit():
            raise TypeError(
                "Elevation must be a digit of type str")
        
        return elev  