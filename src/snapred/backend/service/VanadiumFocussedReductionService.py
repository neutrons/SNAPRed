from typing import Any, Dict, List

from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.data.DataFactoryService import DataFactoryService
from snapred.backend.recipe.VanadiumFocussedReductionRecipe import VanadiumFocussedReductionRecipe
from snapred.backend.service.Service import Service
from snapred.meta.decorators.FromString import FromString
from snapred.meta.decorators.Singleton import Singleton


@Singleton
class VanadiumFocussedReductionService(Service):
    _name = "vanadiumReduction"
    dataFactoryService = DataFactoryService()

    # register the service in ServiceFactory please!
    def __init__(self):
        super().__init__()
        self.registerPath("vanadiumReduction", self.vanadiumReduction)
        return

    def name(self):
        return self._name

    @FromString
    def vanadiumReduction(self, runs: List[RunConfig]) -> Dict[Any, Any]:
        data: Dict[Any, Any] = {}
        for run in runs:
            reductionIngredients = self.dataFactoryService.getReductionIngredients(run.runNumber)
            try:
                VanadiumFocussedReductionRecipe().executeRecipe(reductionIngredients)
            except:
                raise
        return data
