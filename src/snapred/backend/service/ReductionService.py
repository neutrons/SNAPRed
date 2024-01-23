from typing import Any, Dict, List

from snapred.backend.dao.request.FarmFreshIngredients import FarmFreshIngredients
from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.recipe.GenericRecipe import ReductionRecipe
from snapred.backend.service.Service import Service
from snapred.backend.service.SousChef import SousChef
from snapred.meta.decorators.FromString import FromString
from snapred.meta.decorators.Singleton import Singleton


@Singleton
class ReductionService(Service):
    # register the service in ServiceFactory please!
    def __init__(self):
        super().__init__()
        self.sousChef = SousChef()
        self.registerPath("", self.reduce)
        return

    @staticmethod
    def name():
        return "reduction"

    @FromString
    def reduce(self, runs: List[RunConfig]) -> Dict[Any, Any]:
        data: Dict[Any, Any] = {}
        # TODO: collect runs by state then by calibration of state, execute sets of runs by calibration of thier state
        for run in runs:
            farmFresh = FarmFreshIngredients(
                runNumber=run.runNumber,
                useLiteMode=run.useLiteMode,
                focusGroup={"name": "Column", "definition": "path/to/column/definition"},  # TODO FIX THIS
            )
            reductionIngredients = self.sousChef.prepReductionIngredients(farmFresh)
            # TODO: Refresh workspaces
            # import json
            # data[run.runNumber] = json.dumps(reductionIngredients.__dict__, default=lambda o: o.__dict__)
            try:
                data = ReductionRecipe().executeRecipe(ReductionIngredients=reductionIngredients)
            except:
                raise
        return data
