from typing import Dict, List

from pydantic import BaseModel, validator

from snapred.backend.dao.GroupPeakList import GroupPeakList
from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.dao.state.PixelGroup import PixelGroup
from snapred.meta.Config import Config


class DiffractionCalibrationIngredients(BaseModel):
    """Class to hold the ingredients to diffraction calibration"""

    runConfig: RunConfig
    pixelGroup: PixelGroup
    groupedPeakLists: List[GroupPeakList]
    convergenceThreshold: float
    maxOffset: float = Config["calibration.diffraction.maximumOffset"]
    units: str = "TOF"

    @validator("units")
    def validatedSpacing(cls, v):
        if v not in ["TOF", "dSpacing"]:
            raise ValueError(f"dSpacing must be either 'TOF' or 'dSpacing', not '{v}'")
        return v
