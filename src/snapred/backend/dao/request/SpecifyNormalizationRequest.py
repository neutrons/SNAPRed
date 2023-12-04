from typing import List, Optional

from pydantic import BaseModel


class SpecifyNormalizationRequest(BaseModel):
    runNumber: str
    backgroundRunNumber: str
    smoothingParameter: float
    samplePath: str
    focusGroupPath: str
    workspaces: List[str]
