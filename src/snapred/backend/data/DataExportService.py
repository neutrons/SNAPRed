import time
from pathlib import Path

from pydantic import validate_call

from snapred.backend.dao.calibration.Calibration import Calibration
from snapred.backend.dao.calibration.CalibrationIndexEntry import CalibrationIndexEntry
from snapred.backend.dao.calibration.CalibrationRecord import CalibrationRecord
from snapred.backend.dao.normalization.Normalization import Normalization
from snapred.backend.dao.normalization.NormalizationIndexEntry import NormalizationIndexEntry
from snapred.backend.dao.normalization.NormalizationRecord import NormalizationRecord
from snapred.backend.dao.reduction import ReductionRecord
from snapred.backend.dao.state.CalibrantSample.CalibrantSamples import CalibrantSamples
from snapred.backend.data.LocalDataService import LocalDataService
from snapred.meta.Config import Config
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

    def getFullLiteDataFilePath(self, runNumber: str) -> Path:
        path = self.dataService.getIPTS(runNumber)
        pre = "nexus.lite.prefix"
        ext = "nexus.lite.extension"
        fileName = Config[pre] + str(runNumber) + Config[ext]
        return Path(path, fileName)

    def getUniqueTimestamp(self) -> time.struct_time:
        return self.dataService.getUniqueTimestamp()

    ##### REDUCTION METHODS #####

    # NOTE will be added shortly

    ##### CALIBRATION METHODS #####

    @validate_call
    def initializeState(self, runId: str, useLiteMode: bool, name: str):
        return self.dataService.initializeState(runId, useLiteMode, name)

    def exportCalibrationIndexEntry(self, entry: CalibrationIndexEntry):
        self.dataService.writeCalibrationIndexEntry(entry)

    def exportCalibrationRecord(self, record: CalibrationRecord):
        return self.dataService.writeCalibrationRecord(record)

    def exportCalibrationWorkspaces(self, record: CalibrationRecord):
        return self.dataService.writeCalibrationWorkspaces(record)

    def exportCalibrationState(self, calibration: Calibration):
        return self.dataService.writeCalibrationState(calibration)

    ##### NORMALIZATION METHODS #####

    def exportNormalizationIndexEntry(self, entry: NormalizationIndexEntry):
        self.dataService.writeNormalizationIndexEntry(entry)

    def exportNormalizationRecord(self, record: NormalizationRecord):
        return self.dataService.writeNormalizationRecord(record)

    def exportNormalizationWorkspaces(self, record: NormalizationRecord):
        return self.dataService.writeNormalizationWorkspaces(record)

    def exportNormalizationState(self, normalization: Normalization):
        return self.dataService.writeNormalizationState(normalization)

    ##### REDUCTION METHODS #####

    def exportReductionRecord(self, record: ReductionRecord):
        self.dataService.writeReductionRecord(record)

    def exportReductionData(self, record: ReductionRecord):
        self.dataService.writeReductionData(record)

    ##### WORKSPACE METHODS #####

    # @validate_call # until further notice: does not work with 'WorkspaceName'
    def exportWorkspace(self, path: Path, filename: Path, workspaceName: WorkspaceName):
        """
        Write a MatrixWorkspace (derived) workspace to disk in nexus format.
        """
        return self.dataService.writeWorkspace(path, filename, workspaceName)

    # @validate_call # until further notice: does not work with 'WorkspaceName'
    def exportRaggedWorkspace(self, path: Path, filename: Path, workspaceName: WorkspaceName):
        """
        Write a MatrixWorkspace (derived) workspace to disk in nexus format.
        """
        return self.dataService.writeRaggedWorkspace(path, filename, workspaceName)
