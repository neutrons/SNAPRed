import os
from typing import Dict

from snapred.backend.dao.InstrumentConfig import InstrumentConfig
from snapred.backend.dao.ReductionState import ReductionState
from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.dao.StateConfig import StateConfig
from snapred.backend.data.GroceryService import GroceryService
from snapred.backend.data.LocalDataService import LocalDataService
from snapred.meta.decorators.Singleton import Singleton


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

    def fileExists(self, filepath: str) -> bool:
        return self.lookupService.fileExists(filepath)

    def getRunConfig(self, runId: str) -> RunConfig:  # noqa: ARG002
        return self.lookupService.readRunConfig(runId)

    def getInstrumentConfig(self, runId: str) -> InstrumentConfig:  # noqa: ARG002
        return self.lookupService.readInstrumentConfig()

    def getStateConfig(self, runId: str, useLiteMode: bool) -> StateConfig:  # noqa: ARG002
        return self.lookupService.readStateConfig(runId, useLiteMode)

    def constructStateId(self, runId):
        return self.lookupService._generateStateId(runId)

    def getCalibrantSample(self, filePath):
        return self.lookupService.readCalibrantSample(filePath)

    def getCifFilePath(self, sampleId):
        return self.lookupService.readCifFilePath(sampleId)

    def getCalibrationState(self, runId, useLiteMode):
        return self.lookupService.readCalibrationState(runId, useLiteMode)

    def getNormalizationState(self, runId):
        return self.lookupService.readNormalizationState(runId)

    def writeNormalizationState(self, runId):
        return self.lookupService.writeNormalizationState(runId)

    def getWorkspaceForName(self, name):
        return self.groceryService.getWorkspaceForName(name)

    def getCloneOfWorkspace(self, name, copy):
        return self.groceryService.getCloneOfWorkspace(name, copy)

    def getCalibrationDataWorkspace(self, runId, version, name):
        path = self.getCalibrationDataPath(runId, version)
        return self.groceryService.fetchWorkspace(os.path.join(path, name) + ".nxs", name)

    def getWorkspaceCached(self, runId: str, useLiteMode: bool):
        return self.groceryService.fetchNeutronDataCached(runId, useLiteMode)

    def getWorkspaceSingleUse(self, runId: str, useLiteMode: bool):
        return self.groceryService.fetchNeutronDataSingleUse(runId, useLiteMode)

    def getCalibrationRecord(self, runId, version: str = None, useLiteMode: bool = False):
        return self.lookupService.readCalibrationRecord(runId, version, useLiteMode)

    def getNormalizationRecord(self, runId, useLiteMode: bool):
        return self.lookupService.readNormalizationRecord(runId, useLiteMode)

    def getCalibrationIndex(self, runId: str, useLiteMode: bool):
        return self.lookupService.readCalibrationIndex(runId, useLiteMode)

    def getGroupingMap(self, runId: str):
        return self.lookupService.readGroupingMap(runId)

    def checkCalibrationStateExists(self, runId: str):
        return self.lookupService.checkCalibrationFileExists(runId)

    def getSamplePaths(self):
        return self.lookupService.readSamplePaths()

    def getCalibrationDataPath(self, runId: str, version: str):
        return self.lookupService._constructCalibrationDataPath(runId, version)

    def workspaceDoesExist(self, name):
        return self.groceryService.workspaceDoesExist(name)

    # TODO: these are _write_ methods: move to `DataExportService` via `LocalDataService`
    def deleteWorkspace(self, name):
        return self.groceryService.deleteWorkspace(name)

    def deleteWorkspaceUnconditional(self, name):
        return self.groceryService.deleteWorkspaceUnconditional(name)
