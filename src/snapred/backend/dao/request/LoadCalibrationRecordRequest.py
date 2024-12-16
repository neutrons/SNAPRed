from pydantic import BaseModel

from snapred.backend.dao import RunConfig
from snapred.backend.dao.indexing.Versioning import Version


class LoadCalibrationRecordRequest(BaseModel):
    runConfig: RunConfig
    version: Version
