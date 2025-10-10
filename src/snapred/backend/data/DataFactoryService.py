import os
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from pydantic import validate_call

from snapred.backend.dao.calibration.CalibrationRecord import CalibrationRecord
from snapred.backend.dao.indexing.IndexEntry import IndexEntry
from snapred.backend.dao.indexing.Versioning import Version, VersionState
from snapred.backend.dao.normalization.NormalizationRecord import NormalizationRecord
from snapred.backend.dao.reduction import ReductionRecord
from snapred.backend.dao.ReductionState import ReductionState
from snapred.backend.dao.request.CalibrationExportRequest import CalibrationExportRequest
from snapred.backend.dao.request.CreateIndexEntryRequest import CreateIndexEntryRequest
from snapred.backend.dao.request.NormalizationExportRequest import NormalizationExportRequest
from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.dao.RunMetadata import RunMetadata
from snapred.backend.dao.state.DetectorState import DetectorState
from snapred.backend.dao.state.InstrumentConfig import InstrumentConfig
from snapred.backend.dao.StateConfig import StateConfig
from snapred.backend.data.GroceryService import GroceryService
from snapred.backend.data.LocalDataService import LocalDataService
from snapred.meta.decorators.Singleton import Singleton
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName


@Singleton
class DataFactoryService:
    # TODO: rules for busting cache
    cache: Dict[str, ReductionState] = {}

    def __init__(self, lookupService: LocalDataService = None, groceryService: GroceryService = None) -> None:
        # 'LocalDataService' and 'GroceryService' are singletons:
        #   declare them here as instance attributes, rather than class attributes,
        #   to allow singleton reset during testing.
        self.lookupService = self._defaultClass(lookupService, LocalDataService)
        self.groceryService = self._defaultClass(groceryService, GroceryService)

    def _defaultClass(self, val, clazz):
        if val is None:
            val = clazz()
        return val

    ##### MISCELLANEOUS #####

    def fileExists(self, filepath: str) -> bool:
        return self.lookupService.fileExists(filepath)

    def getRunConfig(self, runId: str) -> RunConfig:
        return self.lookupService.readRunConfig(runId)

    def getInstrumentConfig(self, runId: str) -> InstrumentConfig:
        return self.lookupService.readInstrumentConfig(runId)

    def getStateConfig(self, runId: str, useLiteMode: bool) -> StateConfig:
        return self.lookupService.readStateConfig(runId, useLiteMode)

    def constructStateId(self, runId: str) -> Tuple[str, DetectorState]:
        return self.lookupService.generateStateId(runId)

    def stateExists(self, runId: str):
        return self.lookupService.stateExists(runId)

    def getCalibrantSample(self, filePath):
        return self.lookupService.readCalibrantSample(filePath)

    def getCifFilePath(self, sampleId):
        return self.lookupService.readCifFilePath(sampleId)

    def getSampleFilePaths(self):
        return self.lookupService.readSampleFilePaths()

    def getGroupingMap(self, runId: str):
        return self.lookupService.readGroupingMap(runId)

    def getDefaultGroupingMap(self):
        return self.lookupService.readDefaultGroupingMap()

    def getDefaultInstrumentState(self, runId: str):
        return self.lookupService.generateInstrumentState(runId)

    def getCompatibleStates(self, runId: str, useLiteMode: bool):
        return self.lookupService.findCompatibleStates(runId, useLiteMode)

    ##### CALIBRATION METHODS #####

    def calibrationExists(self, runId: str, useLiteMode: bool, state: str):
        return self.lookupService.calibrationExists(runId, useLiteMode, state)

    def getNextCalibrationVersion(self, useLiteMode: bool, state: str) -> Version:
        """
        Returns the next version number for a calibration record.
        If no calibration exists, returns VersionState.NEXT.
        """
        return self.lookupService.calibrationIndexer(useLiteMode, state).nextVersion()

    @validate_call
    def getCalibrationDataPath(self, useLiteMode: bool, version: Version, state: Optional[str] = None):
        return self.lookupService.calibrationIndexer(useLiteMode, state=state).versionPath(version)

    def checkCalibrationStateExists(self, runId: str):
        return self.lookupService.checkCalibrationFileExists(runId)

    def createCalibrationIndexEntry(self, request: CreateIndexEntryRequest) -> IndexEntry:
        return self.lookupService.createCalibrationIndexEntry(request)

    def createCalibrationRecord(self, request: CalibrationExportRequest) -> CalibrationRecord:
        return self.lookupService.createCalibrationRecord(request)

    @validate_call
    def getCalibrationState(self, runId: str, useLiteMode: bool, state: str):
        return self.lookupService.readCalibrationState(runId, useLiteMode, state=state)

    @validate_call
    def getCalibrationIndex(self, useLiteMode: bool, state: str) -> List[IndexEntry]:
        return self.lookupService.calibrationIndexer(useLiteMode, state).getIndex()

    @validate_call
    def getCalibrationRecord(
        self,
        runId: str,
        useLiteMode: bool,
        version: Version = VersionState.LATEST,
        state: Optional[str] = None,
    ) -> CalibrationRecord:
        """
        If no version is passed, will use the latest version applicable to runId
        """
        return self.lookupService.readCalibrationRecord(runId, useLiteMode, state, version)

    @validate_call
    def getCalibrationDataWorkspace(self, useLiteMode: bool, version: Version, name: str, state: str):
        path = self.lookupService.calibrationIndexer(useLiteMode, state).versionPath(version)
        return self.groceryService.fetchWorkspace(os.path.join(path, name) + ".nxs", name)

    @validate_call
    def getLatestApplicableCalibrationVersion(self, runId: str, useLiteMode: bool, state: str):
        return self.lookupService.calibrationIndexer(useLiteMode, state).latestApplicableVersion(runId)

    ##### NORMALIZATION METHODS #####

    def normalizationExists(self, runId: str, useLiteMode: bool, state: str):
        return self.lookupService.normalizationExists(runId, useLiteMode, state)

    @validate_call
    def getNormalizationDataPath(self, useLiteMode: bool, version: Version, state: str):
        return self.lookupService.normalizationIndexer(useLiteMode, state).versionPath(version)

    def createNormalizationIndexEntry(self, request: NormalizationExportRequest) -> IndexEntry:
        return self.lookupService.createNormalizationIndexEntry(request)

    def createNormalizationRecord(self, request: NormalizationExportRequest) -> NormalizationRecord:
        return self.lookupService.createNormalizationRecord(request)

    @validate_call
    def getNormalizationState(self, runId: str, useLiteMode: bool):
        return self.lookupService.readNormalizationState(runId, useLiteMode)

    @validate_call
    def getNormalizationIndex(self, useLiteMode: bool, state: str) -> List[IndexEntry]:
        return self.lookupService.normalizationIndexer(useLiteMode, state).getIndex()

    @validate_call
    def getNormalizationRecord(
        self, runId: str, useLiteMode: bool, state: str, version: Version = VersionState.LATEST
    ) -> NormalizationRecord:
        """
        If no version is passed, will use the latest version applicable to runId
        """
        return self.lookupService.readNormalizationRecord(runId, useLiteMode, state, version)

    @validate_call
    def getNormalizationDataWorkspace(self, useLiteMode: bool, version: Version, state: str, name: str):
        path = self.getNormalizationDataPath(useLiteMode, version, state)
        return self.groceryService.fetchWorkspace(os.path.join(path, name) + ".nxs", name)

    @validate_call
    def getLatestApplicableNormalizationVersion(self, runId: str, useLiteMode: bool, state: str):
        return self.lookupService.normalizationIndexer(useLiteMode, state).latestApplicableVersion(runId)

    ##### REDUCTION METHODS #####

    @validate_call
    def getReductionState(self, runId: str, useLiteMode: bool) -> ReductionState:
        reductionState: ReductionState = None

        if runId in self.cache:
            reductionState = self.cache[runId]
        else:
            # lookup and package data
            reductionState = ReductionState(
                instrumentConfig=self.getInstrumentConfig(runId),
                stateConfig=self.getStateConfig(runId, useLiteMode),
            )
            self.cache[runId] = reductionState

        return reductionState

    @validate_call
    def getReductionDataPath(self, runId: str, useLiteMode: bool, timestamp: float) -> Path:
        return self.lookupService._constructReductionDataPath(runId, useLiteMode, timestamp)

    @validate_call
    def getReductionRecord(self, runId: str, useLiteMode: bool, timestamp: float) -> ReductionRecord:
        return self.lookupService.readReductionRecord(runId, useLiteMode, timestamp)

    @validate_call
    def getReductionData(self, runId: str, useLiteMode: bool, timestamp: float) -> ReductionRecord:
        return self.lookupService.readReductionData(runId, useLiteMode, timestamp)

    @validate_call
    def getCompatibleReductionMasks(self, runId: str, useLiteMode: bool) -> List[WorkspaceName]:
        # Assemble a list of masks, both resident and otherwise, that are compatible with the current reduction
        return self.lookupService.getCompatibleReductionMasks(runId, useLiteMode)

    @validate_call
    def getCompatibleResidentPixelMasks(self, runId: str, useLiteMode: bool) -> List[WorkspaceName]:
        return self.lookupService.getCompatibleResidentPixelMasks(runId, useLiteMode)

    ##### WORKSPACE METHODS #####

    def workspaceDoesExist(self, name):
        return self.groceryService.workspaceDoesExist(name)

    def getWorkspaceForName(self, name: WorkspaceName):
        return self.groceryService.getWorkspaceForName(name)

    def getCloneOfWorkspace(self, name: WorkspaceName, copy: WorkspaceName):
        return self.groceryService.getCloneOfWorkspace(name, copy)

    def getWorkspaceCached(self, runId: str, useLiteMode: bool):
        return self.groceryService.fetchNeutronDataCached(runId, useLiteMode)

    def getWorkspaceSingleUse(self, runId: str, useLiteMode: bool):
        return self.groceryService.fetchNeutronDataSingleUse(runId, useLiteMode)

    # TODO: these are _write_ methods: move to `DataExportService` via `LocalDataService`
    def deleteWorkspace(self, name):
        return self.groceryService.deleteWorkspace(name)

    def deleteWorkspaceUnconditional(self, name):
        return self.groceryService.deleteWorkspaceUnconditional(name)

    ##### RUN-METADATA SUPPORT METHODS #####

    def getRunMetadata(self, runId: str) -> RunMetadata:
        return self.lookupService.readRunMetadata(runId)

    ##### LIVE-DATA SUPPORT METHODS #####

    def hasLiveDataConnection(self) -> bool:
        """For 'live data' methods: test if there is a listener connection to the instrument."""
        return self.lookupService.hasLiveDataConnection()

    def getLiveMetadata(self) -> RunMetadata:
        return self.lookupService.readLiveMetadata()
