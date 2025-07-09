from pydantic import BaseModel


class CalibrationLockRequest(BaseModel):
    runNumber: str
    useLiteMode: bool
