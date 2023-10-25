from typing import Optional

from pydantic import BaseModel

from snapred.backend.dao.state.CalibrantSample.CalibrantSamples import CalibrantSamples


class CalibrationNormalizationRequest(BaseModel):
    runNumber: str
    cifPath: str
    smoothingParameter: Optional[float]
    calibrantSample: CalibrantSamples
