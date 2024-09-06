from pydantic import BaseModel

from snapred.backend.error.ContinueWarning import ContinueWarning


class CalibrationWritePermissionsRequest(BaseModel):
    runNumber: str
    continueFlags: ContinueWarning.Type
