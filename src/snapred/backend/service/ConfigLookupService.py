from snapred.meta.Singleton import Singleton
from snapred.backend.data.DataFactoryService import DataFactoryService

@Singleton
class ConfigLookupService:
    dataFactoryService = DataFactoryService()
    # register the service in ServiceFactory
    def __init__(self):
        return

    def executeRecipe(self, recipeConfig):
        data = []
        for run in recipeConfig.runs:
            data.append(self.dataFactoryService.getReductionState(run.runId))
        return data 

