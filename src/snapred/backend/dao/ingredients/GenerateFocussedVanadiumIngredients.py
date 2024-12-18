from typing import Optional

from pydantic import BaseModel

from snapred.backend.dao.GroupPeakList import GroupPeakList
from snapred.backend.dao.ingredients.ArtificialNormalizationIngredients import ArtificialNormalizationIngredients
from snapred.backend.dao.state.PixelGroup import PixelGroup
from snapred.meta.Config import Config


class GenerateFocussedVanadiumIngredients(BaseModel):
    """Class to hold the ingredients for smoothing preprocessed vanadium data"""

    smoothingParameter: float = Config["calibration.parameters.default.smoothing"]
    pixelGroup: PixelGroup
    # This can be None if we lack a calibration
    detectorPeaks: Optional[list[GroupPeakList]] = None
    artificialNormalizationIngredients: Optional[ArtificialNormalizationIngredients] = None
