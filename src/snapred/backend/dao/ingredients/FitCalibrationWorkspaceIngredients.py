from typing import List

from pydantic import BaseModel

from snapred.backend.dao.CrystallographicInfo import CrystallographicInfo
from snapred.backend.dao.state.InstrumentState import InstrumentState
from snapred.backend.dao.state.PixelGroupingParameters import PixelGroupingParameters


class FitCalibrationWorkspaceIngredients(BaseModel):
    """Class to hold the ingredients necessary for pixel grouping parameter calculation."""

    instrumentState: InstrumentState
    crystalInfo: CrystallographicInfo
    pixelGroupingParameters: List[PixelGroupingParameters]
    workspaceName: str
