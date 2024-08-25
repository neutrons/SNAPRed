from typing import Optional

from pydantic import BaseModel

from snapred.backend.dao.Limit import Limit, Pair
from snapred.backend.dao.state.FocusGroup import FocusGroup
from snapred.backend.error.ContinueWarning import ContinueWarning
from snapred.meta.Config import Config


class NormalizationRequest(BaseModel, extra="forbid"):
    """

    This class encapsulates all the necessary parameters to request a normalization process,
    including default values from application.yml for ease of use and consistency across runs. It
    is designed to provide a comprehensive and customizable framework for requesting normalization
    calibrations, facilitating precise and tailored calibration processes.

    """

    runNumber: str
    backgroundRunNumber: str
    useLiteMode: bool
    focusGroup: FocusGroup
    calibrantSamplePath: str
    smoothingParameter: float = Config["calibration.parameters.default.smoothing"]
    crystalDBounds: Limit[float] = Limit(
        minimum=Config["constants.CrystallographicInfo.crystalDMin"],
        maximum=Config["constants.CrystallographicInfo.crystalDMax"],
    )
    nBinsAcrossPeakWidth: int = Config["calibration.diffraction.nBinsAcrossPeakWidth"]
    fwhmMultipliers: Pair[float] = Pair.model_validate(Config["calibration.parameters.default.FWHMMultiplier"])

    continueFlags: Optional[ContinueWarning.Type] = ContinueWarning.Type.UNSET
