from typing import Optional

from pydantic import BaseModel, Field

from snapred.backend.dao.Limit import Limit, Pair
from snapred.backend.dao.state.FocusGroup import FocusGroup
from snapred.backend.error.ContinueWarning import ContinueWarning
from snapred.meta.Config import Config
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName


class NormalizationRequest(BaseModel, extra="forbid", arbitrary_types_allowed=True):
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
    smoothingParameter: float = Field(default_factory=lambda: Config["calibration.parameters.default.smoothing"])
    crystalDBounds: Limit[float] = Field(
        default_factory=lambda: Limit(
            minimum=Config["constants.CrystallographicInfo.crystalDMin"],
            maximum=Config["constants.CrystallographicInfo.crystalDMax"],
        )
    )
    nBinsAcrossPeakWidth: int = Field(default_factory=lambda: Config["calibration.diffraction.nBinsAcrossPeakWidth"])
    fwhmMultipliers: Pair[float] = Field(
        default_factory=lambda: Pair.model_validate(Config["calibration.parameters.default.FWHMMultiplier"])
    )

    continueFlags: Optional[ContinueWarning.Type] = ContinueWarning.Type.UNSET
    correctedVanadiumWs: Optional[WorkspaceName] = None
