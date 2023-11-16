from typing import Dict

from snapred.backend.dao.ingredients import ReductionIngredients
from snapred.backend.dao.InstrumentConfig import InstrumentConfig
from snapred.backend.dao.ReductionState import ReductionState
from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.dao.state import PixelGroupingParameters
from snapred.backend.dao.StateConfig import StateConfig
from snapred.backend.data.LocalDataService import LocalDataService
from snapred.backend.data.LocalWorkspaceDataService import LocalWorkspaceDataService
from snapred.meta.decorators.Singleton import Singleton


@Singleton
class DataFactoryService:
    lookupService: "LocalDataService"  # Optional[LocalDataService]
    workspaceService: "LocalWorkspaceDataService"  # Optional[LocalWorkspaceDataService]
    # TODO: rules for busting cache
    cache: Dict[str, ReductionState] = {}

    def __init__(
        self, lookupService: LocalDataService = None, workspaceService: LocalWorkspaceDataService = None
    ) -> None:
        self.lookupService = self.defaultClass(lookupService, LocalDataService)
        self.workspaceService = self.defaultClass(workspaceService, LocalWorkspaceDataService)

    def defaultClass(self, val, clazz):
        if val is None:
            val = clazz()
        return val

    def getReductionIngredients(
        self, runId: str, pixelGroupingParameters: PixelGroupingParameters
    ) -> ReductionIngredients:
        return ReductionIngredients(
            reductionState=self.getReductionState(runId),
            runConfig=self.getRunConfig(runId),
            pixelGroupingParameters=pixelGroupingParameters,
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

    def getCalibrantSample(self, sampleId):
        return self.lookupService.readCalibrantSample(sampleId)

    def getCifFilePath(self, sampleId):
        return self.lookupService.readCifFilePath(sampleId)

    def getCalibrationState(self, runId):
        return self.lookupService.readCalibrationState(runId)

    def getWorkspaceForName(self, name):
        return self.workspaceService.getWorkspaceForName(name)

    def getWorkspaceCached(self, runId: str, name: str):
        return self.workspaceService.readWorkspaceCached(runId, name)

    def getCalibrationRecord(self, runId):
        return self.lookupService.readCalibrationRecord(runId)

    def getFocusGroups(self, runId: str):
        return self.lookupService._readFocusGroups(runId)

    def checkCalibrationStateExists(self, runId: str):
        return self.lookupService.checkCalibrationFileExists(runId)

    def getSamplePaths(self):
        return self.lookupService.readSamplePaths()

    def getGroupingFiles(self):
        return self.lookupService.readGroupingFiles()
