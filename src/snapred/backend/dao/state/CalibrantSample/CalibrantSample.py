import datetime
from typing import Optional

from pydantic import BaseModel, field_validator

from snapred.backend.dao.state.CalibrantSample.Crystallography import Crystallography
from snapred.backend.dao.state.CalibrantSample.Geometry import Geometry
from snapred.backend.dao.state.CalibrantSample.Material import Material


class CalibrantSample(BaseModel):
    """Class containing all information used for a Calibrant Sample"""

    name: str
    unique_id: str
    date: Optional[str] = None
    geometry: Geometry
    material: Material
    crystallography: Optional[Crystallography] = None

    # TODO[pydantic]: We couldn't refactor the `validator`, please replace it by `field_validator` manually.
    # Check https://docs.pydantic.dev/dev-v2/migration/#changes-to-validators for more information.

    # @field_validator("date", pre=True, always=True, allow_reuse=True)

    @field_validator("date", mode="before")
    @classmethod
    def set_datetime(cls, v: str) -> str:
        return v or str(datetime.datetime.now())
