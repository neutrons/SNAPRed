from typing import Any, Dict, List

from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.data.DataFactoryService import DataFactoryService
from snapred.backend.recipe.ReductionRecipe import ReductionRecipe
from snapred.backend.service.Service import Service
from snapred.meta.decorators.FromString import FromString
from snapred.meta.decorators.Singleton import Singleton


@Singleton
class ReductionService(Service):
    dataFactoryService = "DataFactoryService"

    # register the service in ServiceFactory please!
    def __init__(self):
        super().__init__()
        self.dataFactoryService = DataFactoryService()
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
            reductionIngredients = self.dataFactoryService.getReductionIngredients(run.runNumber)
            # TODO: Refresh workspaces
            # import json
            # data[run.runNumber] = json.dumps(reductionIngredients.__dict__, default=lambda o: o.__dict__)
            try:
                ReductionRecipe().executeRecipe(reductionIngredients)
            except:
                raise
        return data
