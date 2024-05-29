from typing import Dict, List

from pydantic import BaseModel

from snapred.backend.dao.Limit import Pair
from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.dao.state.FocusGroup import FocusGroup
from snapred.meta.Config import Config
from snapred.meta.mantid.AllowedPeakTypes import ALLOWED_PEAK_TYPES
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName, WorkspaceType


class CalibrationAssessmentRequest(BaseModel):
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
    workspaces: Dict[WorkspaceType, List[WorkspaceName]]
    # fiddly bits
    peakFunction: ALLOWED_PEAK_TYPES
    crystalDMin: float
    crystalDMax: float
    peakIntensityThreshold: float
    nBinsAcrossPeakWidth: int
    fwhmMultipliers: Pair[float] = Pair.parse_obj(Config["calibration.parameters.default.FWHMMultiplier"])
    maxChiSq: float
