from typing import Dict

from snapred.backend.dao.ReductionState import ReductionState
from snapred.backend.dao.SNAPRequest import SNAPRequest
from snapred.backend.data.DataFactoryService import DataFactoryService
from snapred.meta.Singleton import Singleton

# from snapred.backend.recipe.ReductionRecipe


@Singleton
class ConfigLookupService:
    dataFactoryService = DataFactoryService()

    # register the service in ServiceFactory
    def __init__(self):
        return

    def orchestrateRecipe(self, request: SNAPRequest) -> Dict[str, ReductionState]:
        data = {}
        for run in request.runs:
            data[run.runId] = self.dataFactoryService.getReductionState(run.runId)
        return data
