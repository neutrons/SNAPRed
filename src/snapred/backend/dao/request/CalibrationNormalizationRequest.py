from typing import Optional

from pydantic import BaseModel

from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.dao.state.CalibrantSample.CalibrantSamples import CalibrantSamples


class CalibrationNormalizationRequest(BaseModel):
    run: RunConfig
    cifPath: str
    smoothingParameter: Optional[float]
    calibrantSample: CalibrantSamples
