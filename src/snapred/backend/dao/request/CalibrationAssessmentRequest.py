from typing import Optional

from pydantic import BaseModel

from snapred.backend.dao.RunConfig import RunConfig


class CalibrationAssessmentRequest(BaseModel):
    """
    Request object to initiate a calibration assessment of a given run.
    This is compared against the known crystal data provided via cif file
    """

    run: RunConfig
    cifPath: str
    smoothingParameter: Optional[float]
