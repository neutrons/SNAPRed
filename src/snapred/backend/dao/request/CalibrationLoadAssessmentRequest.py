from typing import Optional

from pydantic import BaseModel

from snapred.backend.dao.RunConfig import RunConfig
from snapred.meta.Config import Config


class CalibrationLoadAssessmentRequest(BaseModel):
    """

    The CalibrationLoadAssessmentRequest class is crafted to initiate the generation
    and loading of an assessment for a specified calibration version linked to a run's
    instrument state. It specifies a runId and version to identify the calibration of
    interest, along with a checkExistent flag that, when true, avoids regenerating the
    assessment if it already exists.

    """

    runId: str
    version: str
    checkExistent: bool  # if true, do not generate assessment if it already exists
