from typing import Any, Dict

from snapred.backend.dao.VanadiumReductionIngredients import VanadiumReductionIngredients
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
    def vanadiumReduction(self, vanadiumReductionIngredients: VanadiumReductionIngredients) -> Dict[Any, Any]:
        data: Dict[Any, Any] = {}
        run = vanadiumReductionIngredients.run
        reductionIngredients = self.dataFactoryService.getReductionIngredients(run.runNumber)
        smoothIngredients = vanadiumReductionIngredients.smoothIngredients
        try:
            VanadiumFocussedReductionRecipe().executeRecipe(
                reductionIngredients=reductionIngredients, smoothIngredients=smoothIngredients
            )
        except:
            raise
        return data
