import os
from pathlib import Path
from typing import Dict, Optional

from pydantic import validate_call

from snapred.backend.dao.InstrumentConfig import InstrumentConfig
from snapred.backend.dao.reduction import ReductionRecord
from snapred.backend.dao.ReductionState import ReductionState
from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.dao.StateConfig import StateConfig
from snapred.backend.data.GroceryService import GroceryService
from snapred.backend.data.LocalDataService import LocalDataService
from snapred.meta.decorators.Singleton import Singleton
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName


@Singleton
class DataFactoryService:
    lookupService: "LocalDataService"  # Optional[LocalDataService]
    groceryService: "GroceryService"
    # TODO: rules for busting cache
    cache: Dict[str, ReductionState] = {}

    def __init__(self, lookupService: LocalDataService = None, groceryService: GroceryService = None) -> None:
        self.lookupService = self._defaultClass(lookupService, LocalDataService)
        self.groceryService = self._defaultClass(groceryService, GroceryService)

    def _defaultClass(self, val, clazz):
        if val is None:
            val = clazz()
        return val

    ##### MISCELLANEOUS #####

    def fileExists(self, filepath: str) -> bool:
        return self.lookupService.fileExists(filepath)

    def getRunConfig(self, runId: str) -> RunConfig:  # noqa: ARG002
        return self.lookupService.readRunConfig(runId)

    def getInstrumentConfig(self, runId: str) -> InstrumentConfig:  # noqa: ARG002
        return self.lookupService.readInstrumentConfig()

    def getStateConfig(self, runId: str, useLiteMode: bool) -> StateConfig:  # noqa: ARG002
        return self.lookupService.readStateConfig(runId, useLiteMode)

    def constructStateId(self, runId: str):
        return self.lookupService._generateStateId(runId)

    def getCalibrantSample(self, filePath):
        return self.lookupService.readCalibrantSample(filePath)

    def getCifFilePath(self, sampleId):
        return self.lookupService.readCifFilePath(sampleId)

    def getSampleFilePaths(self):
        return self.lookupService.readSampleFilePaths()

    def getGroupingMap(self, runId: str):
        return self.lookupService.readGroupingMap(runId)

    ##### CALIBRATION METHODS #####

    @validate_call
    def getCalibrationDataPath(self, runId: str, useLiteMode: bool, version: int):
        return self.lookupService._constructCalibrationDataPath(runId, useLiteMode, version)

    def checkCalibrationStateExists(self, runId: str):
        return self.lookupService.checkCalibrationFileExists(runId)

    @validate_call
    def getCalibrationState(self, runId: str, useLiteMode: bool):
        return self.lookupService.readCalibrationState(runId, useLiteMode)

    @validate_call
    def getCalibrationIndex(self, runId: str, useLiteMode: bool):
        return self.lookupService.readCalibrationIndex(runId, useLiteMode)

    @validate_call
    def getCalibrationRecord(self, runId: str, useLiteMode: bool, version: Optional[int] = None):
        return self.lookupService.readCalibrationRecord(runId, useLiteMode, version)

    @validate_call
    def getCalibrationDataWorkspace(self, runId: str, useLiteMode: bool, version: int, name: str):
        path = self.getCalibrationDataPath(runId, useLiteMode, version)
        return self.groceryService.fetchWorkspace(os.path.join(path, name) + ".nxs", name)

    ##### NORMALIZATION METHODS #####

    @validate_call
    def getNormalizationDataPath(self, runId: str, useLiteMode: bool, version: int):
        return self.lookupService._constructNormalizationDataPath(runId, useLiteMode, version)

    @validate_call
    def getNormalizationState(self, runId: str, useLiteMode: bool):
        return self.lookupService.readNormalizationState(runId, useLiteMode)

    @validate_call
    def getNormalizationIndex(self, runId: str, useLiteMode: bool):
        return self.lookupService.readNormalizationIndex(runId, useLiteMode)

    @validate_call
    def getNormalizationRecord(self, runId: str, useLiteMode: bool, version: Optional[int] = None):
        return self.lookupService.readNormalizationRecord(runId, useLiteMode, version)

    @validate_call
    def getNormalizationDataWorkspace(self, runId: str, useLiteMode: bool, version: int, name: str):
        path = self.getNormalizationDataPath(runId, useLiteMode, version)
        return self.groceryService.fetchWorkspace(os.path.join(path, name) + ".nxs", name)
    
    @validate_call
    def getNormalizationVersion(self, runId: str, useLiteMode: bool):
        return self.lookupService.getVersionFromNormalizationIndex(runId, useLiteMode)

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
    def getReductionDataPath(self, runId: str, useLiteMode: bool, version: str) -> Path:
        return self.lookupService._constructReductionDataPath(runId, useLiteMode, version)

    @validate_call
    def getReductionRecord(self, runId: str, useLiteMode: bool, version: Optional[int] = None) -> ReductionRecord:
        return self.lookupService.readReductionRecord(runId, useLiteMode, version)

    @validate_call
    def getReductionData(self, runId: str, useLiteMode: bool, version: int) -> ReductionRecord:
        return self.lookupService.readReductionData(runId, useLiteMode, version)

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
