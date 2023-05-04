from snapred.backend.data.DataFactoryService import DataFactoryService
from snapred.backend.recipe.CalibrationReductionRecipe import CalibrationReductionRecipe
from snapred.meta.Singleton import Singleton


@Singleton
class CalibrationReductionService:
    name = "calibration"
    dataFactoryService = DataFactoryService()

    # register the service in ServiceFactory please!
    def __init__(self):
        return

    def orchestrateRecipe(self, reductionRequest):
        data = {}
        # TODO: collect runs by state then by calibration of state, execute sets of runs by calibration of thier state
        for run in reductionRequest.runs:
            reductionIngredients = self.dataFactoryService.getReductionIngredients(run.runNumber)
            # TODO: Refresh workspaces
            # import json
            # data[run.runNumber] = json.dumps(reductionIngredients.__dict__, default=lambda o: o.__dict__)
            try:
                CalibrationReductionRecipe().executeRecipe(reductionIngredients)
            except:
                raise
        return data
