from snapred.backend.dao.calibration.Calibration import Calibration
from snapred.backend.dao.calibration.CalibrationIndexEntry import CalibrationIndexEntry
from snapred.backend.dao.calibration.CalibrationRecord import CalibrationRecord
from snapred.backend.dao.state.CalibrantSample.CalibrantSamples import CalibrantSamples
from snapred.backend.data.LocalDataService import LocalDataService
from snapred.meta.decorators.Singleton import Singleton


@Singleton
class DataExportService:
    dataService: "LocalDataService"  # Optional[LocalDataService]

    def __init__(self, dataService: LocalDataService = None) -> None:
        if dataService:
            self.dataService = dataService
        else:
            self.dataService = LocalDataService()

    def exportCalibrationIndexEntry(self, entry: CalibrationIndexEntry):
        self.dataService.writeCalibrationIndexEntry(entry)

    def exportCalibrationRecord(self, record: CalibrationRecord):
        return self.dataService.writeCalibrationRecord(record)

    def exportCalibrationReductionResult(self, runId: str, workspaceName: str):
        return self.dataService.writeCalibrationReductionResult(runId, workspaceName)

    def writeCalibrantSampleFile(self, entry: CalibrantSamples):
        self.dataService.writeCalibrantSample(entry)

    def exportCalibrationState(self, runId: str, calibration: Calibration):
        return self.dataService.writeCalibrationState(runId, calibration)

    def initializeState(self, runId: str, name: str):
        return self.dataService.initializeState(runId, name)

    def deleteWorkspace(self, workspaceName: str):
        return self.dataService.deleteWorkspace(workspaceName)
