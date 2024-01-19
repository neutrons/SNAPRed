from typing import List, Optional

from pydantic import BaseModel

from snapred.backend.dao.GroupPeakList import GroupPeakList
from snapred.backend.dao.ingredients.PeakIngredients import PeakIngredients
from snapred.backend.dao.state.CalibrantSample import CalibrantSamples
from snapred.backend.dao.state.PixelGroup import PixelGroup


class NormalizationCalibrationIngredients(BaseModel):
    """Class to hold the ingredients necessary for normalization calibration workflow"""

    pixelGroup: PixelGroup
    calibrantSample: CalibrantSamples
    detectorPeaks: Optional[List[GroupPeakList]]
    detectorPeakIngredients: Optional[PeakIngredients]
