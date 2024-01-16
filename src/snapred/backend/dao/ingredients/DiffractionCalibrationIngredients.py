from typing import Dict, List

from pydantic import BaseModel

from snapred.backend.dao.GroupPeakList import GroupPeakList
from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.dao.state.FocusGroup import FocusGroup
from snapred.backend.dao.state.InstrumentState import InstrumentState
from snapred.backend.dao.state.PixelGroup import PixelGroup
from snapred.meta.Config import Config


class DiffractionCalibrationIngredients(BaseModel):
    """Class to hold the ingredients to diffraction calibration"""

    runConfig: RunConfig
    # TODO Potentially remove use of InstrumentState
    instrumentState: InstrumentState
    focusGroup: FocusGroup
    pixelGroup: PixelGroup
    groupedPeakLists: List[GroupPeakList]
    calPath: str
    convergenceThreshold: float
    maxOffset: float = Config["calibration.diffraction.maximumOffset"]
