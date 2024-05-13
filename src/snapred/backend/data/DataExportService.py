from pathlib import Path

from pydantic import validate_arguments

from snapred.backend.dao.calibration.Calibration import Calibration
from snapred.backend.dao.calibration.CalibrationIndexEntry import CalibrationIndexEntry
from snapred.backend.dao.calibration.CalibrationRecord import CalibrationRecord
from snapred.backend.dao.normalization.NormalizationIndexEntry import NormalizationIndexEntry
from snapred.backend.dao.normalization.NormalizationRecord import NormalizationRecord
from snapred.backend.dao.state.CalibrantSample.CalibrantSamples import CalibrantSamples
from snapred.backend.data.LocalDataService import LocalDataService
from snapred.meta.decorators.Singleton import Singleton
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName


@Singleton
class DataExportService:
    dataService: "LocalDataService"

    def __init__(self, dataService: LocalDataService = None) -> None:
        self.dataService = self._defaultClass(dataService, LocalDataService)

    def _defaultClass(self, val, clazz):
        if val is None:
            val = clazz()
        return val

    def exportCalibrationIndexEntry(self, entry: CalibrationIndexEntry):
        self.dataService.writeCalibrationIndexEntry(entry)

    def exportCalibrationRecord(self, record: CalibrationRecord):
        return self.dataService.writeCalibrationRecord(record)

    def exportCalibrationWorkspaces(self, record: CalibrationRecord):
        return self.dataService.writeCalibrationWorkspaces(record)

    def exportCalibrantSampleFile(self, entry: CalibrantSamples):
        self.dataService.writeCalibrantSample(entry)

    def exportCalibrationState(self, calibration: Calibration):
        return self.dataService.writeCalibrationState(calibration)

    def exportNormalizationIndexEntry(self, entry: NormalizationIndexEntry):
        self.dataService.writeNormalizationIndexEntry(entry)

    def exportNormalizationRecord(self, record: NormalizationRecord):
        return self.dataService.writeNormalizationRecord(record)

    def exportNormalizationWorkspaces(self, record: NormalizationRecord):
        return self.dataService.writeNormalizationWorkspaces(record)

    @validate_arguments
    def exportWorkspace(self, path: Path, filename: Path, workspaceName: WorkspaceName):
        """
        Write a MatrixWorkspace (derived) workspace to disk in nexus format.
        """
        return self.dataService.writeWorkspace(path, filename, workspaceName)

    @validate_arguments
    def exportRaggedWorkspace(self, path: Path, filename: Path, workspaceName: WorkspaceName):
        """
        Write a MatrixWorkspace (derived) workspace to disk in nexus format.
        """
        return self.dataService.writeRaggedWorkspace(path, filename, workspaceName)

    @validate_arguments
    def initializeState(self, runId: str, useLiteMode: bool, name: str):
        return self.dataService.initializeState(runId, useLiteMode, name)
