from pydantic import BaseModel
from snapred.backend.model.RunConfig import RunConfig


class CalibrationAssessmentRequest(BaseModel):
    run: RunConfig
    cifPath: str
