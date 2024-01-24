from typing import Optional

from pydantic import BaseModel

from snapred.backend.dao.CrystallographicInfo import CrystallographicInfo
from snapred.backend.dao.state.InstrumentState import InstrumentState
from snapred.backend.dao.state.PixelGroup import PixelGroup


class PeakIngredients(BaseModel):
    """Class to hold the ingredients for various peak manipulation algorithms"""

    instrumentState: InstrumentState
    crystalInfo: CrystallographicInfo
    pixelGroup: PixelGroup
    peakIntensityThreshold: float
    smoothingParameter: Optional[float]
