from typing import Optional

from pydantic import BaseModel

from snapred.backend.dao.Limit import Limit
from snapred.meta.Config import Config


class FarmFreshIngredients(BaseModel):
    """
    from these, the Sous Chef can make everything
    """

    # NOTE these exsit because i got sick of passing the same
    # list of six arguments around in all of the Sous Chef methods

    runNumber: str
    useLiteMode: bool
    groupingSchema: str
    calibrantSamplePath: Optional[str]
    cifPath: Optional[str]
    groupingSchemaFilepath: Optional[str]
    # convergenceThreshold: Optional[float]
    dBounds: Optional[Limit[float]]  # d-spacing min/max for xtal calculation
    nBinsAcrossPeakWidth: int = Config["calibration.diffraction.nBinsAcrossPeakWidth"]
    peakIntensityThreshold: float = Config["calibration.diffraction.peakIntensityThreshold"]
