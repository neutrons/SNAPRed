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
        # TODO: collect runs by state then by calibration of state, execute sets of runs by calibration of thier state
        for run in reductionRequest.runs:
            reductionIngredients = self.dataFactoryService.getReductionIngredients(run.runId)
            # TODO: Refresh workspaces
            data[run.runId] = ReductionRecipe().executeRecipe(reductionIngredients)
        return data 

