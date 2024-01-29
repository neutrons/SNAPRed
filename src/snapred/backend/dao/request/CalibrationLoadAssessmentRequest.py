from typing import Optional

from pydantic import BaseModel

from snapred.backend.dao.RunConfig import RunConfig
from snapred.meta.Config import Config


class CalibrationLoadAssessmentRequest(BaseModel):
    """
    Request object to generate and load an assessment for a given calibration version
    of the instrument state associated with a given run
    """

    runId: str
    version: str
    checkExistent: bool  # if true, do not generate assessment if it already exists
