from typing import Any, Dict

from pydantic import BaseModel, ConfigDict, field_validator

from snapred.backend.dao.Limit import Pair
from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.dao.state.FocusGroup import FocusGroup
from snapred.meta.Config import Config
from snapred.meta.mantid.AllowedPeakTypes import PeakFunctionEnum
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName, WorkspaceType


class CalibrationAssessmentRequest(BaseModel, extra="forbid"):
    """

    The CalibrationAssessmentRequest class is crafted to streamline the process of initiating
    a calibration assessment for a specific run, set against standard crystal data typically
    provided through a cif file. It incorporates a run configuration, mapping various workspaces
    by their type to workspace names for analytical context, and specifies a focusGroup for targeted
    assessment. The calibrantSamplePath points to the sample data, while useLiteMode, nBinsAcrossPeakWidth,
    peakIntensityThreshold, and peakFunction define the assessment's operational parameters, with defaults set
    according to system configurations.

    """

    run: RunConfig
    useLiteMode: bool
    focusGroup: FocusGroup
    calibrantSamplePath: str
    workspaces: Dict[WorkspaceType, WorkspaceName]
    # fiddly bits
    peakFunction: PeakFunctionEnum
    crystalDMin: float
    crystalDMax: float
    peakIntensityThreshold: float
    nBinsAcrossPeakWidth: int
    fwhmMultipliers: Pair[float] = Pair.model_validate(Config["calibration.parameters.default.FWHMMultiplier"])
    maxChiSq: float = Config["constants.GroupDiffractionCalibration.MaxChiSq"]

    @field_validator("fwhmMultipliers", mode="before")
    @classmethod
    def validate_fwhmMultipliers(cls, v: Any) -> Pair[float]:
        if isinstance(v, dict):
            v = Pair[float](**v)
        if not isinstance(v, Pair[float]):
            # Coerce Generic[T]-derived type
            v = Pair[float](**v.dict())
        return v

    model_config = ConfigDict(
        extra="forbid",
        # required in order to use 'WorkspaceName'
        arbitrary_types_allowed=True,
    )
