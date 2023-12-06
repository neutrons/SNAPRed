from typing import Dict, List

from pydantic import BaseModel

from snapred.backend.dao.GroupPeakList import GroupPeakList
from snapred.backend.dao.PixelGroup import PixelGroup
from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.dao.state.FocusGroup import FocusGroup
from snapred.backend.dao.state.InstrumentState import InstrumentState


class DiffractionCalibrationIngredients(BaseModel):
    """Class to hold the ingredients to diffraction calibration"""

    runConfig: RunConfig
    instrumentState: InstrumentState
    focusGroup: FocusGroup
    pixelGroup: PixelGroup
    groupedPeakLists: List[GroupPeakList]
    calPath: str
    convergenceThreshold: float
    maxOffset: float = 2.0
