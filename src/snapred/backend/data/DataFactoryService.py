from pdb import run
from typing import Dict

from snapred.backend.dao import calibration
from snapred.backend.dao.ingredients import ReductionIngredients
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

    def getReductionIngredients(self, runId: str, pixelGroup: PixelGroup = None) -> ReductionIngredients:
        if pixelGroup is None:
            calibration = self.getCalibrationState(runId)
            if calibration is None or calibration.instrumentState.pixelGroup is None:
                stateId = self.constructStateId(runId)
                raise RuntimeError(f"Pixel group not found for runId {runId} and stateId {stateId}")

            pixelGroup = calibration.instrumentState.pixelGroup

        return ReductionIngredients(
            reductionState=self.getReductionState(runId),
            runConfig=self.getRunConfig(runId),
            pixelGroup=pixelGroup,
        )

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

    def loadCalibrationDataWorkspace(self, runId, version, name):
        path = self.getCalibrationDataPath(runId, version)
        return self.groceryService.fetchWorkspace(path, name)

    def writeWorkspace(self, path, name):
        return self.groceryService.writeWorkspace(path, name)

    def getWorkspaceCached(self, runId: str, useLiteMode: bool):
        return self.groceryService.fetchNeutronDataCached(runId, useLiteMode)

    def getWorkspaceSingleUse(self, runId: str, useLiteMode: bool):
        return self.groceryService.fetchNeutronDataSingleUse(runId, useLiteMode)

    def getCalibrationRecord(self, runId):
        return self.lookupService.readCalibrationRecord(runId)

    def getNormalizationRecord(self, runId):
        return self.lookupService.readNormalizationRecord(runId)

    def getCalibrationIndex(self, runId: str):
        return self.lookupService.readCalibrationIndex(runId)

    def getFocusGroups(self, runId: str):
        return self.lookupService._readFocusGroups(runId)

    def checkCalibrationStateExists(self, runId: str):
        return self.lookupService.checkCalibrationFileExists(runId)

    def getSamplePaths(self):
        return self.lookupService.readSamplePaths()

    def getGroupingFiles(self):
        return self.lookupService.readGroupingFiles()

    def getCalibrationDataPath(self, runId: str, version: str):
        return self.lookupService._constructCalibrationDataPath(runId, version)
