from typing import Dict, List

from pydantic import BaseModel

from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.dao.state.FocusGroup import FocusGroup
from snapred.meta.Config import Config
from snapred.meta.mantid.AllowedPeakTypes import ALLOWED_PEAK_TYPES
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName, WorkspaceType


class CalibrationAssessmentRequest(BaseModel):
    """
    Request object to initiate a calibration assessment of a given run.
    This is compared against the known crystal data provided via cif file
    """

    run: RunConfig
    workspaces: Dict[WorkspaceType, List[WorkspaceName]]
    focusGroup: FocusGroup
    calibrantSamplePath: str
    useLiteMode: bool
    nBinsAcrossPeakWidth: int = Config["calibration.diffraction.nBinsAcrossPeakWidth"]
    peakIntensityThreshold: float = Config["calibration.diffraction.peakIntensityThreshold"]
    peakType: ALLOWED_PEAK_TYPES = "Gaussian"
