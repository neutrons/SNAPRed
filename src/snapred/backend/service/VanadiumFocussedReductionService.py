from typing import Any, Dict

from snapred.backend.dao.ingredients import VanadiumReductionIngredients
from snapred.backend.data.DataFactoryService import DataFactoryService

# from snapred.backend.recipe.VanadiumFocussedReductionRecipe import VanadiumFocussedReductionRecipe
from snapred.backend.recipe.GenericRecipe import VanadiumFocussedReductionRecipe
from snapred.backend.service.Service import Service
from snapred.meta.decorators.FromString import FromString
from snapred.meta.decorators.Singleton import Singleton


@Singleton
class VanadiumFocussedReductionService(Service):
    def __init__(self):
        super().__init__()
        self.dataFactoryService = DataFactoryService()
        self.registerPath("vanadiumReduction", self.vanadiumReduction)
        return

    @staticmethod
    def name():
        return "vanadiumReduction"

    @FromString
    def vanadiumReduction(self, vanadiumReductionIngredients: VanadiumReductionIngredients) -> Dict[Any, Any]:
        data: Dict[Any, Any] = {}
        run = vanadiumReductionIngredients.run
        reductionIngredients = self.dataFactoryService.getReductionIngredients(run.runNumber)
        smoothIngredients = vanadiumReductionIngredients.smoothIngredients
        try:
            VanadiumFocussedReductionRecipe().executeRecipe(
                reductionIngredients=reductionIngredients, SmoothDataIngredients=smoothIngredients
            )
        except:
            raise
        return data
