from typing import List

from pydantic import BaseModel

from snapred.backend.dao.GroupPeakList import GroupPeakList
from snapred.backend.dao.state.CalibrantSample import CalibrantSample
from snapred.backend.dao.state.PixelGroup import PixelGroup


class NormalizationIngredients(BaseModel):
    """Class to hold the ingredients necessary for normalization calibration workflow"""

    pixelGroup: PixelGroup
    calibrantSample: CalibrantSample
    detectorPeaks: List[GroupPeakList]
