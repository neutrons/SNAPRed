from typing import Optional

from pydantic import BaseModel

from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.dao.state.FocusGroup import FocusGroup
from snapred.meta.Config import Config


class CalibrationAssessmentRequest(BaseModel):
    """
    Request object to initiate a calibration assessment of a given run.
    This is compared against the known crystal data provided via cif file
    """

    run: RunConfig
    workspace: str
    focusGroup: FocusGroup
    calibrantSamplePath: str
    useLiteMode: bool
    nBinsAcrossPeakWidth: int = Config["calibration.diffraction.nBinsAcrossPeakWidth"]
