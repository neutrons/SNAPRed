from pathlib import Path
from typing import Optional

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

# TODO these are export methods, they should not return anything


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

    ##### CALIBRATION METHODS #####

    @validate_call
    def initializeState(self, runId: str, useLiteMode: bool, name: str):
        return self.dataService.initializeState(runId, useLiteMode, name)

    def exportCalibrationIndexEntry(self, entry: CalibrationIndexEntry):
        """
        Entry must have correct version set.
        """
        self.dataService.writeCalibrationIndexEntry(entry)

    def exportCalibrationRecord(self, record: CalibrationRecord):
        """
        Record must have correct version set.
        """
        self.dataService.writeCalibrationRecord(record)

    def exportCalibrationWorkspaces(self, record: CalibrationRecord):
        """
        Record must have correct version set and workspace names finalized.
        """
        self.dataService.writeCalibrationWorkspaces(record)

    def exportCalibrationState(self, calibration: Calibration):
        """
        Calibration must have correct version set.
        """
        self.dataService.writeCalibrationState(calibration)

    ##### NORMALIZATION METHODS #####

    def exportNormalizationIndexEntry(self, entry: NormalizationIndexEntry):
        """
        Entry must have correct version set.
        """
        self.dataService.writeNormalizationIndexEntry(entry)

    def exportNormalizationRecord(self, record: NormalizationRecord):
        """
        Record must have correct version set.
        """
        self.dataService.writeNormalizationRecord(record)

    def exportNormalizationWorkspaces(self, record: NormalizationRecord):
        """
        Record must have correct version set and workspace names finalized.
        """
        self.dataService.writeNormalizationWorkspaces(record)

    def exportNormalizationState(self, normalization: Normalization):
        """
        Normalization must have correct version set.
        """
        self.dataService.writeNormalizationState(normalization)

    ##### REDUCTION METHODS #####

    def exportReductionRecord(self, record: ReductionRecord, version: Optional[int] = None):
        """
        If no version given, will save at current time
        """
        return self.dataService.writeReductionRecord(record, version)

    def exportReductionData(self, record: ReductionRecord, version: Optional[int] = None):
        """
        If no version given, will save at current time
        """
        return self.dataService.writeReductionData(record, version)

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
