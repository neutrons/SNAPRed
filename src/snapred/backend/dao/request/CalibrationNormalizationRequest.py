from typing import Optional

from pydantic import BaseModel


class CalibrationNormalizationRequest(BaseModel):
    runNumber: str
    cifPath: str
    smoothingParameter: Optional[float]
