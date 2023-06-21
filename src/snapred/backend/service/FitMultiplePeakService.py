from typing import Any, Dict

from snapred.backend.recipe.FitMultiplePeaksRecipe import FitMultiplePeaksRecipe
from snapred.backend.dao.FitMultiplePeaksIngredients import FitMultiplePeaksIngredients
from snapred.backend.service.Service import Service
from snapred.meta.decorators.FromString import FromString
from snapred.meta.decorators.Singleton import Singleton


@Singleton
class FitMultiplePeaksService(Service):
    _name = "fitMultiplePeaks"

    def __init__(self):
        super().__init__()
        self.registerPath("fitMultiplePeaks", self.fit_multiple_peaks)
        return

    def name(self):
        return self._name

    @FromString
    def fit_multiple_peaks(self, fitMultiplePeaksIngredients: FitMultiplePeaksIngredients) -> Dict[Any, Any]:
        data: Dict[Any, Any] = {}
        try:
            data = FitMultiplePeaksRecipe().executeRecipe(fitMultiplePeaksIngredients)
        except:
            raise
        return data


