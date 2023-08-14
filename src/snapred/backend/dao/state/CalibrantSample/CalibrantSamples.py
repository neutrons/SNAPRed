import datetime
from typing import Optional

from pydantic import BaseModel, validator

from snapred.backend.dao.state.CalibrantSample.Crystallography import Crystallography
from snapred.backend.dao.state.CalibrantSample.Geometry import Geometry
from snapred.backend.dao.state.CalibrantSample.Material import Material


class CalibrantSamples(BaseModel):
    """Class containing all information used for a Calibrant Sample"""

    name: str
    unique_id: str
    date: Optional[str] = None
    geometry: Geometry
    material: Material
    crystallography: Crystallography

    @validator("date", pre=True, always=True, allow_reuse=True)
    def set_datetime(cls, v):
        return v or str(datetime.datetime.now())
