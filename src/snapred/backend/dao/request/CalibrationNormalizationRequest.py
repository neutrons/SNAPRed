from typing import Optional

from pydantic import BaseModel

from snapred.backend.dao.state.CalibrantSample.CalibrantSamples import CalibrantSamples
from snapred.backend.dao.RunConfig import RunConfig

class CalibrationNormalizationRequest(BaseModel):
    run: RunConfig
    cifPath: str
    smoothingParameter: Optional[float]
    calibrantSample: CalibrantSamples
