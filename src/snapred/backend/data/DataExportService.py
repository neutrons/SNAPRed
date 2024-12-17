import time
from pathlib import Path
from typing import Optional, Tuple

from pydantic import validate_call

from snapred.backend.dao.calibration.Calibration import Calibration
from snapred.backend.dao.calibration.CalibrationRecord import CalibrationRecord
from snapred.backend.dao.indexing.IndexEntry import IndexEntry
from snapred.backend.dao.normalization.Normalization import Normalization
from snapred.backend.dao.normalization.NormalizationRecord import NormalizationRecord
from snapred.backend.dao.reduction import ReductionRecord
from snapred.backend.dao.state.CalibrantSample.CalibrantSample import CalibrantSample
from snapred.backend.data.LocalDataService import LocalDataService
from snapred.meta.Config import Config
from snapred.meta.decorators.Singleton import Singleton
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName

# TODO these are export methods, they should not return anything


@Singleton
class DataExportService:
    def __init__(self, dataService: LocalDataService = None) -> None:
        # 'LocalDataService' is a singleton: declare it as an instance attribute, rather than a class attribute,
        #   to allow singleton reset during testing.
        self.dataService = self._defaultClass(dataService, LocalDataService)

    def _defaultClass(self, val, clazz):
        if val is None:
            val = clazz()
        return val

    ##### MISCELLANEOUS #####

    def exportCalibrantSampleFile(self, entry: CalibrantSample):
        self.dataService.writeCalibrantSample(entry)

    def getFullLiteDataFilePath(self, runNumber: str) -> Path:
        path = self.dataService.getIPTS(runNumber)
        pre = "nexus.lite.prefix"
        ext = "nexus.lite.extension"
        fileName = Config[pre] + str(runNumber) + Config[ext]
        return Path(path, fileName)

    def getUniqueTimestamp(self) -> time.struct_time:
        return self.dataService.getUniqueTimestamp()

    def checkFileandPermission(self, filePath: Path) -> Tuple[bool, bool]:
        return self.dataService.checkFileandPermission(filePath)

    def checkWritePermissions(self, path: Path) -> bool:
        return self.dataService.checkWritePermissions(path)

    ##### CALIBRATION METHODS #####

    @validate_call
    def initializeState(self, runId: str, useLiteMode: bool, name: str):
        return self.dataService.initializeState(runId, useLiteMode, name)

    def exportCalibrationIndexEntry(self, entry: IndexEntry):
        """
        Entry must have correct version set.
        """
        self.dataService.writeCalibrationIndexEntry(entry)

    def exportCalibrationRecord(self, record: CalibrationRecord, entry: Optional[IndexEntry] = None):
        """
        Record must have correct version set.
        """
        self.dataService.writeCalibrationRecord(record, entry)

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

    def getCalibrationStateRoot(self, runNumber: str) -> Path:
        stateId, _ = self.dataService.generateStateId(runNumber)
        return self.dataService.constructCalibrationStateRoot(stateId)

    ##### NORMALIZATION METHODS #####

    def exportNormalizationIndexEntry(self, entry: IndexEntry):
        """
        Entry must have correct version set.
        """
        self.dataService.writeNormalizationIndexEntry(entry)

    def exportNormalizationRecord(self, record: NormalizationRecord, entry: Optional[IndexEntry] = None):
        """
        Record must have correct version set.
        """
        self.dataService.writeNormalizationRecord(record, entry)

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

    def exportReductionRecord(self, record: ReductionRecord):
        """
        Record must have correct timestamp and workspace names finalized.
        - side effect: creates output directories when required.
        """
        self.dataService.writeReductionRecord(record)

    def exportReductionData(self, record: ReductionRecord):
        """
        Record must have correct timestamp and workspace names finalized:
        - 'exportReductionRecord' must have been called previously.
        """
        self.dataService.writeReductionData(record)

    def getReductionStateRoot(self, runNumber: str) -> Path:
        return self.dataService._constructReductionStateRoot(runNumber)

    ##### WORKSPACE METHODS #####

    # @validate_call # until further notice: does not work with 'WorkspaceName'
    def exportWorkspace(self, path: Path, filename: Path, workspaceName: WorkspaceName):
        """
        Write a MatrixWorkspace-derived workspace to disk in nexus format.
        """
        return self.dataService.writeWorkspace(path, filename, workspaceName)
