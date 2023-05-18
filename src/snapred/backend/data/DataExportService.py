from snapred.backend.dao.calibration.CalibrationIndexEntry import CalibrationIndexEntry
from snapred.backend.dao.calibration.CalibrationRecord import CalibrationRecord
from snapred.backend.data.LocalDataService import LocalDataService
from snapred.meta.decorators.Singleton import Singleton


@Singleton
class DataExportService:
    dataService: LocalDataService  # Optional[LocalDataService]

    def __init__(self, dataService: LocalDataService = LocalDataService()) -> None:
        self.dataService = dataService

    def exportCalibrationIndexEntry(self, entry: CalibrationIndexEntry):
        self.dataService.writeCalibrationIndexEntry(entry)

    def exportCalibrationRecord(self, record: CalibrationRecord):
        return self.dataService.writeCalibrationRecord(record)
