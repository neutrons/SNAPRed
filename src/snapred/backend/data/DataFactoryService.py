import os
from pathlib import Path
from typing import Dict, Optional

from pydantic import validate_arguments

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

    ##### REDUCTION METHODS #####

    @validate_arguments
    def getReductionState(self, runId: str, useLiteMode: bool) -> ReductionState:
        reductionState: ReductionState

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

    ##### CALIBRATION METHODS #####

    @validate_arguments
    def getCalibrationDataPath(self, runId: str, useLiteMode: bool, version: int):
        return self.lookupService.calibrationIndex(runId, useLiteMode).versionPath(version)

    def checkCalibrationStateExists(self, runId: str):
        return self.lookupService.checkCalibrationFileExists(runId)

    @validate_arguments
    def getCalibrationState(self, runId: str, useLiteMode: bool):
        return self.lookupService.readCalibrationState(runId, useLiteMode)

    @validate_arguments
    def getCalibrationIndex(self, runId: str, useLiteMode: bool):
        return self.lookupService.calibrationIndex(runId, useLiteMode).getIndex()

    @validate_arguments
    def getCalibrationRecord(self, runId: str, useLiteMode: bool, version: Optional[int] = None):
        return self.lookupService.readCalibrationRecord(runId, useLiteMode, version)

    @validate_arguments
    def getCalibrationDataWorkspace(self, runId: str, useLiteMode: bool, version: str, name: str):
        path = self.lookupService.calibrationIndex(runId, useLiteMode).versionPath(version)
        return self.groceryService.fetchWorkspace(os.path.join(path, name) + ".nxs", name)

    ##### NORMALIZATION METHODS #####

    @validate_arguments
    def getNormalizationDataPath(self, runId: str, useLiteMode: bool, version: int):
        return self.lookupService.normalizationIndex(runId, useLiteMode).versionPath(version)

    @validate_arguments
    def getNormalizationState(self, runId: str, useLiteMode: bool):
        return self.lookupService.readNormalizationState(runId, useLiteMode)

    @validate_arguments
    def getNormalizationIndex(self, runId: str, useLiteMode: bool):
        return self.lookupService.normalizationIndex(runId, useLiteMode).getIndex()

    @validate_arguments
    def getNormalizationRecord(self, runId: str, useLiteMode: bool, version: Optional[int] = None):
        return self.lookupService.readNormalizationRecord(runId, useLiteMode, version)

    @validate_arguments
    def getNormalizationDataWorkspace(self, runId: str, useLiteMode: bool, version: str, name: str):
        path = self.getNormalizationDataPath(runId, useLiteMode, version)
        return self.groceryService.fetchWorkspace(os.path.join(path, name) + ".nxs", name)

    ##### REDUCTION METHODS #####

    @validate_arguments
    def getReductionDataPath(self, runId: str, useLiteMode: bool, version: str) -> Path:
        return self.lookupService.reductionIndex(runId, useLiteMode).versionPath(version)

    @validate_arguments
    def getReductionRecord(self, runId: str, useLiteMode: bool, version: Optional[int] = None) -> ReductionRecord:
        return self.lookupService.readReductionRecord(runId, useLiteMode, version)

    @validate_arguments
    def getReductionData(self, runId: str, useLiteMode: bool, grouping: str, version: int) -> ReductionRecord:
        return self.lookupService.readReductionData(runId, useLiteMode, grouping, version)

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
