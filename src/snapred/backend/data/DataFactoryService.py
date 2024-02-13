import os
from pdb import run
from typing import Dict

from snapred.backend.dao import calibration
from snapred.backend.dao.InstrumentConfig import InstrumentConfig
from snapred.backend.dao.ReductionState import ReductionState
from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.dao.state.PixelGroup import PixelGroup
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
        self.lookupService = self.defaultClass(lookupService, LocalDataService)
        self.groceryService = self.defaultClass(groceryService, GroceryService)

    def defaultClass(self, val, clazz):
        if val is None:
            val = clazz()
        return val

    def getReductionState(self, runId: str) -> ReductionState:
        reductionState: ReductionState

        if runId in self.cache:
            reductionState = self.cache[runId]

        else:
            # lookup and package data
            reductionState = ReductionState(
                instrumentConfig=self.getInstrumentConfig(runId),
                stateConfig=self.getStateConfig(runId),
            )
            self.cache[runId] = reductionState

        return reductionState

    def getRunConfig(self, runId: str) -> RunConfig:  # noqa: ARG002
        return self.lookupService.readRunConfig(runId)

    def getInstrumentConfig(self, runId: str) -> InstrumentConfig:  # noqa: ARG002
        return self.lookupService.readInstrumentConfig()

    def getStateConfig(self, runId: str) -> StateConfig:  # noqa: ARG002
        return self.lookupService.readStateConfig(runId)

    def constructStateId(self, runId):
        return self.lookupService._generateStateId(runId)

    def getCalibrantSample(self, filePath):
        return self.lookupService.readCalibrantSample(filePath)

    def getCifFilePath(self, sampleId):
        return self.lookupService.readCifFilePath(sampleId)

    def getCalibrationState(self, runId):
        return self.lookupService.readCalibrationState(runId)

    def getNormalizationState(self, runId):
        return self.lookupService.readNormalizationState(runId)

    def writeNormalizationState(self, runId):
        return self.lookupService.writeNormalizationState(runId)

    def getWorkspaceForName(self, name):
        return self.groceryService.getWorkspaceForName(name)

    def getClonedofWorkspace(self, name, copy):
        return self.groceryService.getCloneOfWorkspace(name, copy)

    def workspaceDoesExist(self, name):
        return self.groceryService.workspaceDoesExist(name)

    def deleteWorkspace(self, name):
        return self.groceryService.deleteWorkspace(name)

    def deleteWorkspaceUnconditional(self, name):
        return self.groceryService.deleteWorkspaceUnconditional(name)

    def loadCalibrationDataWorkspace(self, runId, version, wsInfo):
        path = self.getCalibrationDataPath(runId, version)
        path = os.path.join(path, wsInfo.name)
        path += ".nxs"
        if wsInfo.type == "EventWorkspace" or wsInfo.type == "Workspace2D":
            loader = "LoadNexusProcessed"
        else:
            loader = "LoadNexus"
        return self.groceryService.fetchWorkspace(path, wsInfo.name, loader)

    def loadCalibrationTableWorkspaces(self, runId, version):
        path = self.getCalibrationDataPath(runId, version)
        return self.groceryService.readCalibrationTableWorkspaces(path, runId, version)

    def writeWorkspace(self, path, wsInfo, version: str = None):
        return self.groceryService.writeWorkspace(path, wsInfo.name, version)

    def getWorkspaceCached(self, runId: str, useLiteMode: bool):
        return self.groceryService.fetchNeutronDataCached(runId, useLiteMode)

    def getWorkspaceSingleUse(self, runId: str, useLiteMode: bool):
        return self.groceryService.fetchNeutronDataSingleUse(runId, useLiteMode)

    def getCalibrationRecord(self, runId, version: str = None):
        return self.lookupService.readCalibrationRecord(runId, version)

    def getNormalizationRecord(self, runId):
        return self.lookupService.readNormalizationRecord(runId)

    def getCalibrationIndex(self, runId: str):
        return self.lookupService.readCalibrationIndex(runId)

    def getFocusGroups(self):
        # return self.lookupService._readFocusGroups(runId)
        return self.lookupService.readFocusGroups()

    def checkCalibrationStateExists(self, runId: str):
        return self.lookupService.checkCalibrationStateExists(runId)

    def getSamplePaths(self):
        return self.lookupService.readSamplePaths()

    def getGroupingFiles(self):
        return self.lookupService.readGroupingFiles()

    def getCalibrationDataPath(self, runId: str, version: str):
        return self.lookupService._constructCalibrationDataPath(runId, version)
