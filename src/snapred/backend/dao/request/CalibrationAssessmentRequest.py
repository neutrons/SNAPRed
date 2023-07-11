from pydantic import BaseModel
from snapred.backend.dao.RunConfig import RunConfig


class CalibrationAssessmentRequest(BaseModel):
    run: RunConfig
    cifPath: str
