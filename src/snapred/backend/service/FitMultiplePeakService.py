from typing import Any, Dict

from snapred.backend.dao.ingredients import PeakIngredients
from snapred.backend.recipe.GenericRecipe import FitMultiplePeaksRecipe
from snapred.backend.service.Service import Service
from snapred.meta.decorators.FromString import FromString
from snapred.meta.decorators.Singleton import Singleton


@Singleton
class FitMultiplePeaksService(Service):
    def __init__(self):
        super().__init__()
        self.registerPath("fitMultiplePeaks", self.fit_multiple_peaks)
        return

    @staticmethod
    def name():
        return "fitMultiplePeaks"

    @FromString
    def fit_multiple_peaks(self, ingredients: PeakIngredients) -> Dict[Any, Any]:
        data: Dict[Any, Any] = {}
        try:
            data = FitMultiplePeaksRecipe().executeRecipe(ingredients)
        except:
            raise
        return data
