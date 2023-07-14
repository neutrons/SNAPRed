from typing import Any, Dict

from snapred.backend.dao.SmoothDataExcludingPeaksIngredients import SmoothDataExcludingPeaksIngredients
from snapred.backend.recipe.SmoothDataExcludingPeaksRecipe import SmoothDataExcludingPeaksRecipe
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
    
    def name(self):
        return self._name
    
    @FromString
    def smooth_data_excluding_peaks(self, smoothDataExcludingPeaksIngredients: SmoothDataExcludingPeaksIngredients) -> Dict[Any, Any]:
        data: Dict[Any, Any] = {}
        try:
            data = SmoothDataExcludingPeaksRecipe().executeRecipe(smoothDataExcludingPeaksIngredients)
        except:
            raise
        return data