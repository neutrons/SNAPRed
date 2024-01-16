from typing import Dict, List

from pydantic import BaseModel

from snapred.backend.dao.GroupPeakList import GroupPeakList
from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.dao.state.FocusGroup import FocusGroup
from snapred.backend.dao.state.InstrumentState import InstrumentState
from snapred.backend.dao.state.PixelGroup import PixelGroup


class DiffractionCalibrationIngredients(BaseModel):
    """Class to hold the ingredients to diffraction calibration"""

    runConfig: RunConfig
    pixelGroup: PixelGroup
    groupedPeakLists: List[GroupPeakList]
    convergenceThreshold: float
    maxOffset: float = 10.0
