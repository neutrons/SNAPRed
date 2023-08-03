from pydantic import BaseModel

from snapred.backend.dao.calibration.CalibrationIndexEntry import CalibrationIndexEntry
from snapred.backend.dao.calibration.CalibrationRecord import CalibrationRecord


class CalibrationExportRequest(BaseModel):
    """
    Once a calibration is completed and assessed as satisfactory
    a user may request to persist it to disk
    This request is mostly passing back the result of that assessment step.
    """

    calibrationRecord: CalibrationRecord
    calibrationIndexEntry: CalibrationIndexEntry
