from pydantic import BaseModel

from snapred.backend.dao.GroupPeakList import GroupPeakList
from snapred.backend.dao.state.PixelGroup import PixelGroup
from snapred.meta.Config import Config


class GenerateFocussedVanadiumIngredients(BaseModel):
    """Class to hold the ingredients for smoothing preprocessed vanadium data"""

    smoothingParameter: float = Config["calibration.parameters.default.smoothing"]
    pixelGroup: PixelGroup
    detectorPeaks: list[GroupPeakList]
