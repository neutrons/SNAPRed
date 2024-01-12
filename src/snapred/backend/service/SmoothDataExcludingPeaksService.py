from typing import Any, Dict

from snapred.backend.dao.ingredients import PeakIngredients as Ingredients
from snapred.backend.recipe.SmoothDataExcludingPeaksRecipe import SmoothDataExcludingPeaksRecipe as Recipe
from snapred.backend.service.Service import Service
from snapred.meta.decorators.FromString import FromString
from snapred.meta.decorators.Singleton import Singleton


@Singleton
class SmoothDataExcludingPeaksService(Service):
    _name = "smoothDataExcludingPeaks"

    def __init__(self):
        super().__init__()
        self.registerPath("smoothDataExcludingPeaks", self.smooth_data_excluding_peaks)
        return

    @staticmethod
    def name():
        return "smoothDataExcludingPeaks"

    @FromString
    def smooth_data_excluding_peaks(self, ingredients: Ingredients) -> Dict[Any, Any]:
        data: Dict[Any, Any] = {}
        try:
            data = Recipe().executeRecipe(ingredients)
        except:
            raise
        return data
