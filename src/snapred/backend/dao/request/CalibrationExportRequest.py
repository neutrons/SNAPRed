from pydantic import BaseModel
from snapred.backend.dao.calibration.CalibrationIndexEntry import CalibrationIndexEntry
from snapred.backend.dao.calibration.CalibrationRecord import CalibrationRecord


class CalibrationExportRequest(BaseModel):
    calibrationRecord: CalibrationRecord
    calibrationIndexEntry: CalibrationIndexEntry
