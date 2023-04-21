from typing import Dict, List

from snapred.backend.dao.InstrumentConfig import InstrumentConfig
from snapred.backend.dao.ReductionIngredients import ReductionIngredients
from snapred.backend.dao.ReductionState import ReductionState
from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.dao.state.DiffractionCalibrant import DiffractionCalibrant
from snapred.backend.dao.state.FocusGroup import FocusGroup
from snapred.backend.dao.state.NormalizationCalibrant import NormalizationCalibrant
from snapred.backend.dao.StateConfig import StateConfig
from snapred.backend.data.LocalDataService import LocalDataService
from snapred.meta.Singleton import Singleton


@Singleton
class DataFactoryService:
    lookupService: LocalDataService  # Optional[LocalDataService]
    # TODO: rules for busting cache
    cache: Dict[str, ReductionState] = {}

    def __init__(self, lookupService: LocalDataService = LocalDataService()) -> None:
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

    def loadNexusFile(self, reductionState, deepcopy=True) -> None:
        # cacheService.get(filepath)
        # else lookupService.loadFile(filepath);cacheService.put(filepath, data)
        # if deepcopy: clone workspace
        raise NotImplementedError("_loadNexusFile() is not implemented")

    def _getDiffractionCalibrant(self, runId) -> DiffractionCalibrant:  # noqa: ARG002
        raise NotImplementedError("_getDiffractionCalibrant() is not implemented")
        return DiffractionCalibrant()

    def _getNormalizationCalibrant(self, runId) -> NormalizationCalibrant:  # noqa: ARG002
        raise NotImplementedError("_getNormalizationCalibrant() is not implemented")
        return NormalizationCalibrant()

    def _getFocusGroups(self, runId) -> List[FocusGroup]:  # noqa: ARG002
        raise NotImplementedError("_getFocusGroups() is not implemented")
        return [FocusGroup()]

    def constructStateId(self, runId):
        return self.lookupService._generateStateId(self.getRunConfig(runId))

    def _getGetometricConfig(self, runId) -> None:
        raise NotImplementedError("_getGetometricConfig() is not implemented")
        # call additional data service, specify shallow copy
