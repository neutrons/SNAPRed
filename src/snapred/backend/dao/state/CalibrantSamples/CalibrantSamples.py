import datetime
from typing import Optional

from pydantic import BaseModel, validator
from snapred.backend.dao.state.CalibrantSamples import Crystallography, Geometry, Material


class CalibrantSamples(BaseModel):
    """Class containing all information used for a Calibrant Sample"""

    name: str
    unique_id: str
    date: Optional[datetime.datetime] = None
    geometry: Geometry
    material: Material
    crystallography: Crystallography

    @validator("date", pre=True, always=True)
    def set_datetime(cls, v):
        return v or datetime.now()
