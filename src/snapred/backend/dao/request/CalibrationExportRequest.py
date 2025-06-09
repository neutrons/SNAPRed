from pydantic import BaseModel

from snapred.backend.dao.request.CreateCalibrationRecordRequest import CreateCalibrationRecordRequest


class CalibrationExportRequest(BaseModel, extra="forbid"):
    """

    The CalibrationExportRequest class facilitates the process of saving completed and satisfactorily
    assessed calibrations to disk. It acts as a conduit for passing the outcomes of the calibration
    assessment step back to the system, including both the calibrationRecord, which details the
    calibration process and parameters, and the calibrationIndexEntry, which indexes the calibration
    for reference.

    """

    createRecordRequest: CreateCalibrationRecordRequest
