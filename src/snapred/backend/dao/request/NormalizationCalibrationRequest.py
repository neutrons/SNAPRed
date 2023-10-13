from pydantic import BaseModel

from snapred.backend.dao.RunConfig import RunConfig


class NormalizationCalibrationRequest(BaseModel):
    runNumber: RunConfig
    emptyRunNumber: RunConfig
