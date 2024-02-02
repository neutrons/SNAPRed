from typing import Optional

from pydantic import BaseModel, validator

from snapred.backend.dao.Limit import Limit
from snapred.backend.dao.state import FocusGroup
from snapred.meta.Config import Config


class FarmFreshIngredients(BaseModel):
    """
    from these, the Sous Chef can make everything
    """

    # NOTE this class ia a REQUEST object, despite its name
    # Do NOT use inside ingredients for algorithms

    runNumber: str
    useLiteMode: bool
    focusGroup: FocusGroup

    ## needs to be mandatory for diffcal
    cifPath: Optional[str]

    ## needs to be mandatory for normcal
    calibrantSamplePath: Optional[str]

    ## the below are not-so-fresh, being fiddly optional parameters with defaults
    convergenceThreshold: float = Config["calibration.diffraction.convergenceThreshold"]
    nBinsAcrossPeakWidth: int = Config["calibration.diffraction.nBinsAcrossPeakWidth"]
    peakIntensityThreshold: float = Config["calibration.diffraction.peakIntensityThreshold"]
    maxOffset: float = Config["calibration.diffraction.maximumOffset"]
    crystalDBounds: Limit[float] = Limit(
        minimum=Config["constants.CrystallographicInfo.dMin"],
        maximum=Config["constants.CrystallographicInfo.dMax"],
    )
