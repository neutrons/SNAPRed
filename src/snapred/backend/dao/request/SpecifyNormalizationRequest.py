from typing import Optional

from pydantic import BaseModel

from snapred.backend.dao.RunConfig import RunConfig


class SpecifyNormalizationRequest(BaseModel):
    runNumber: RunConfig
    preSmoothedWorkpace: str
    smoothedWorkspace: str
    smoothingParameter: float
    samplePath: Optional[str]
    focusGroupPath: Optional[str]
