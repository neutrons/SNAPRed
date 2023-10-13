from pydantic import BaseModel

from snapred.backend.dao.RunConfig import RunConfig


class SpecifyCalibrationRequest(BaseModel):
    runNumber: RunConfig
    preSmoothedWorkpace: str
    smoothedWorkspace: str
