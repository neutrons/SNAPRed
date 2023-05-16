from typing import Any, Dict

from snapred.backend.data.DataFactoryService import DataFactoryService
from snapred.backend.recipe.CrystallographicInfoRecipe import CrystallographicInfoRecipe
from snapred.meta.Singleton import Singleton


@Singleton
class IngestionService:
    name = "ingestion"
    dataFactoryService = DataFactoryService()

    # register the service in ServiceFactory please!
    def __init__(self):
        return

    def orchestrateRecipe(self, cifPath: str) -> Dict[Any, Any]:
        data: Dict[Any, Any] = {}
        # TODO: collect runs by state then by calibration of state, execute sets of runs by calibration of thier state
        # for run in request.runs:
        # cifPath = self.dataFactoryService.getReductionIngredients(run.runNumber)
        # TODO: Refresh workspaces
        # import json
        # data[run.runNumber] = json.dumps(reductionIngredients.__dict__, default=lambda o: o.__dict__)
        try:
            data = CrystallographicInfoRecipe().executeRecipe(cifPath)
        except:
            raise
        return data
