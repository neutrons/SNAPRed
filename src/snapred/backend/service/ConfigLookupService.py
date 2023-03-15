from snapred.meta.Singleton import Singleton
from snapred.backend.data.DataFactoryService import DataFactoryService
# from snapred.backend.recipe.ReductionRecipe

@Singleton
class ConfigLookupService:
    dataFactoryService = DataFactoryService()
    # register the service in ServiceFactory
    def __init__(self):
        return

    def orchestrateRecipe(self, reductionRequest):
        data = {}
        for run in reductionRequest.runs:
            data[run.runId] = self.dataFactoryService.getReductionState(run.runId)
        return data 

