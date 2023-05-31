from typing import Any, Dict, List

from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.data.DataFactoryService import DataFactoryService  # type: ignore
from snapred.backend.recipe.ExtractionRecipe import ExtractionRecipe
from snapred.backend.service.Service import Service
from snapred.meta.decorators.FromString import FromString
from snapred.meta.decorators.Singleton import Singleton


@Singleton
class ExtractionService(Service):
    _name = "extraction"
    dataFactoryService = DataFactoryService()

    # register the service in ServiceFactory please!
    def __init__(self):
        super().__init__()
        self.registerPath("", self.extractData)
        return

    def name(self):
        return self._name

    @FromString
    def extractData(self, runs: List[RunConfig]) -> Dict[Any, Any]:
        data: Dict[Any, Any] = {}
        # TODO: collect runs by state then by calibration of state, execute sets of runs by calibration of thier state
        for run in runs:
            reductionIngredients = self.dataFactoryService.getReductionIngredients(run.runNumber)
            # TODO: Refresh workspaces
            # import json
            # data[run.runNumber] = json.dumps(reductionIngredients.__dict__, default=lambda o: o.__dict__)
            try:
                ExtractionRecipe().executeRecipe(reductionIngredients)
            except:
                raise
        return data
