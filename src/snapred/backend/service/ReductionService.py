from snapred.meta.Singleton import Singleton
from snapred.backend.data.DataFactoryService import DataFactoryService
from snapred.backend.recipe.ReductionRecipe import ReductionRecipe

@Singleton
class ReductionService:
    dataFactoryService = DataFactoryService()
    # register the service in ServiceFactory please!
    def __init__(self):
        return

    def executeRecipe(self, reductionRequest):
        data = {}
        for run in reductionRequest.runs:
            reductionState = self.dataFactoryService.getReductionState(run.runId)
            data[run.runId] = ReductionRecipe().executeRecipe(reductionState)
        return data 

