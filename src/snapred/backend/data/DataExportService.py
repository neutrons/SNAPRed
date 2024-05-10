from pathlib import Path

from snapred.backend.dao.calibration.Calibration import Calibration
from snapred.backend.dao.calibration.CalibrationIndexEntry import CalibrationIndexEntry
from snapred.backend.dao.calibration.CalibrationRecord import CalibrationRecord
from snapred.backend.dao.normalization.Normalization import Normalization
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

    ##### MISCELLANEOUS #####

    def exportCalibrantSampleFile(self, entry: CalibrantSamples):
        self.dataService.writeCalibrantSample(entry)

    ##### CALIBRATION METHODS #####

    def initializeState(self, runId: str, name: str, useLiteMode: bool):
        return self.dataService.initializeState(runId, name, useLiteMode)

    def exportCalibrationIndexEntry(self, entry: CalibrationIndexEntry, useLiteMode: bool):
        self.dataService.writeCalibrationIndexEntry(entry, useLiteMode)

    def exportCalibrationRecord(self, record: CalibrationRecord):
        return self.dataService.writeCalibrationRecord(record)

    def exportCalibrationWorkspaces(self, record: CalibrationRecord):
        return self.dataService.writeCalibrationWorkspaces(record)

    def exportCalibrationState(self, runId: str, calibration: Calibration):
        return self.dataService.writeCalibrationState(runId, calibration)

    ##### NORMALIZATION METHODS #####

    def exportNormalizationIndexEntry(self, entry: NormalizationIndexEntry, useLiteMode: bool):
        self.dataService.writeNormalizationIndexEntry(entry, useLiteMode)

    def exportNormalizationRecord(self, record: NormalizationRecord):
        return self.dataService.writeNormalizationRecord(record)

    def exportNormalizationWorkspaces(self, record: NormalizationRecord):
        return self.dataService.writeNormalizationWorkspaces(record)

    def exportNormalizationState(self, runId: str, normalization: Normalization):
        return self.dataService.writeNormalizationState(runId, normalization)

    ##### WORKSPACE METHODS #####

    def exportWorkspace(self, path: Path, filename: Path, workspaceName: WorkspaceName):
        """
        Write a MatrixWorkspace (derived) workspace to disk in nexus format.
        """
        return self.dataService.writeWorkspace(path, filename, workspaceName)

    def exportRaggedWorkspace(self, path: Path, filename: Path, workspaceName: WorkspaceName):
        """
        Write a MatrixWorkspace (derived) workspace to disk in nexus format.
        """
        return self.dataService.writeRaggedWorkspace(path, filename, workspaceName)
