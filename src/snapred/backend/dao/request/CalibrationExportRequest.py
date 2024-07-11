from pydantic import BaseModel, model_validator

from snapred.backend.dao.request.CreateCalibrationRecordRequest import CreateCalibrationRecordRequest
from snapred.backend.dao.request.CreateIndexEntryRequest import CreateIndexEntryRequest


class CalibrationExportRequest(BaseModel, extra="forbid"):
    """

    The CalibrationExportRequest class facilitates the process of saving completed and satisfactorily
    assessed calibrations to disk. It acts as a conduit for passing the outcomes of the calibration
    assessment step back to the system, including both the calibrationRecord, which details the
    calibration process and parameters, and the calibrationIndexEntry, which indexes the calibration
    for reference.

    """

    createIndexEntryRequest: CreateIndexEntryRequest
    createRecordRequest: CreateCalibrationRecordRequest

    @model_validator(mode="after")
    def same_version(self):
        if self.createIndexEntryRequest.version != self.createRecordRequest.version:
            raise ValueError(
                f"Version {self.createIndexEntryRequest.version} does not match {self.createRecordRequest.version}"
            )
        else:
            return self
