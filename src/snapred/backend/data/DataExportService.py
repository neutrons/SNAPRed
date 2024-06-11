from pathlib import Path
from typing import Optional

from pydantic import validate_arguments

from snapred.backend.dao.calibration.Calibration import Calibration
from snapred.backend.dao.calibration.CalibrationIndexEntry import CalibrationIndexEntry
from snapred.backend.dao.calibration.CalibrationRecord import CalibrationRecord
from snapred.backend.dao.IndexEntry import IndexEntry
from snapred.backend.dao.normalization.Normalization import Normalization
from snapred.backend.dao.normalization.NormalizationIndexEntry import NormalizationIndexEntry
from snapred.backend.dao.normalization.NormalizationRecord import NormalizationRecord
from snapred.backend.dao.reduction import ReductionRecord
from snapred.backend.dao.state.CalibrantSample.CalibrantSamples import CalibrantSamples
from snapred.backend.data.LocalDataService import LocalDataService
from snapred.meta.Config import Config
from snapred.meta.decorators.Singleton import Singleton
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName

# TODO these are export methods, the should not return anything


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

    @validate_arguments
    def initializeState(self, runId: str, useLiteMode: bool, name: str):
        return self.dataService.initializeState(runId, useLiteMode, name)

    def exportCalibrationIndexEntry(self, entry: CalibrationIndexEntry, version: Optional[int]):
        """
        If no version given, will follow Indexor rules to save at next version
        """
        self.dataService.writeCalibrationIndexEntry(entry, version)

    def exportCalibrationRecord(self, record: CalibrationRecord, version: Optional[int] = None):
        """
        If no version given, will follow Indexor rules to save at next version
        """
        self.dataService.writeCalibrationRecord(record, version)

    def exportCalibrationWorkspaces(self, record: CalibrationRecord, version: Optional[int] = None):
        """
        If no version given, will follow Indexor rules to save at next version
        """
        self.dataService.writeCalibrationWorkspaces(record, version)

    def exportCalibrationState(self, calibration: Calibration, version: Optional[int] = None):
        """
        If no version given, will follow Indexor rules to save at next version
        """
        self.dataService.writeCalibrationState(calibration, version)

    ##### NORMALIZATION METHODS #####

    def exportNormalizationIndexEntry(self, entry: NormalizationIndexEntry, version: Optional[int] = None):
        """
        If no version given, will follow Indexor rules to save at next version
        """
        self.dataService.writeNormalizationIndexEntry(entry, version)

    def exportNormalizationRecord(self, record: NormalizationRecord, version: Optional[int] = None):
        """
        If no version given, will follow Indexor rules to save at next version
        """
        self.dataService.writeNormalizationRecord(record, version)

    def exportNormalizationWorkspaces(self, record: NormalizationRecord, version: Optional[int] = None):
        """
        If no version given, will follow Indexor rules to save at next version
        """
        self.dataService.writeNormalizationWorkspaces(record, version)

    def exportNormalizationState(self, normalization: Normalization, version: Optional[int] = None):
        """
        If no version given, will follow Indexor rules to save at next version
        """
        self.dataService.writeNormalizationState(normalization, version)

    ##### REDUCTION METHODS #####

    def exportReductionIndexEntry(self, entry: IndexEntry, version: Optional[int] = None):
        """
        If no version given, will follow Indexor rules to save at next version
        """
        self.dataService.writeReductionIndexEntry(entry, version)

    def exportReductionRecord(self, record: ReductionRecord, version: Optional[int] = None):
        """
        If no version given, will follow Indexor rules to save at next version
        """
        self.dataService.writeReductionRecord(record, version)

    def exportReductionData(self, record: ReductionRecord, version: Optional[int] = None):
        """
        If no version given, will follow Indexor rules to save at next version
        """
        self.dataService.writeReductionData(record, version)

    ##### WORKSPACE METHODS #####

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
