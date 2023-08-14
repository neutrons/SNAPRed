from typing import Dict

from snapred.backend.dao.InstrumentConfig import InstrumentConfig
from snapred.backend.dao.ReductionIngredients import ReductionIngredients
from snapred.backend.dao.ReductionState import ReductionState
from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.dao.StateConfig import StateConfig
from snapred.backend.data.LocalDataService import LocalDataService
from snapred.meta.decorators.Singleton import Singleton


@Singleton
class DataFactoryService:
    lookupService: "LocalDataService"  # Optional[LocalDataService]
    # TODO: rules for busting cache
    cache: Dict[str, ReductionState] = {}

    def __init__(self, lookupService: LocalDataService = None) -> None:
        if lookupService is None:
            self.lookupService = LocalDataService()
        else:
            self.lookupService = lookupService

    def getReductionIngredients(self, runId: str) -> ReductionIngredients:
        return ReductionIngredients(
            reductionState=self.getReductionState(runId),
            runConfig=self.getRunConfig(runId),
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

    def getCalibrationState(self, runId):
        return self.lookupService.readCalibrationState(runId)

    def getWorkspaceForName(self, name):
        return self.lookupService.getWorkspaceForName(name)

    def getCalibrationRecord(self, runId):
        return self.lookupService.readCalibrationRecord(runId)

    def getFocusGroups(self, runId: str):
        return self.lookupService._readFocusGroups(runId)
