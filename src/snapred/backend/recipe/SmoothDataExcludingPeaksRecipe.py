from typing import Any, Dict

from mantid.api import AlgorithmManager

from snapred.backend.dao.ingredients.SmoothDataExcludingPeaksIngredients import SmoothDataExcludingPeaksIngredients
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.algorithm.SmoothDataExcludingPeaksAlgo import (
    name as SmoothDataExcludingPeaksAlgo,
)
from snapred.meta.decorators.Singleton import Singleton

logger = snapredLogger.getLogger(__name__)


@Singleton
class SmoothDataExcludingPeaksRecipe:
    smoothDataExcludingPeaksAlgoName: str = SmoothDataExcludingPeaksAlgo

    def __init__(self):
        pass

    def executeRecipe(self, smoothDataExcludingPeaksIngredients: SmoothDataExcludingPeaksIngredients):
        logger.info("Executing SmoothDataExcludingPeaksRecipe")
        data: Dict[str, Any] = {}

        algo = AlgorithmManager.create(self.smoothDataExcludingPeaksAlgoName)
        algo.setProperty("SmoothDataExcludingPeaksIngredients", smoothDataExcludingPeaksIngredients.json())
        try:
            data["result"] = algo.execute()
        except RuntimeError as e:
            errorString = str(e)
            raise Exception(errorString.split("\n")[0])
        logger.info("SmoothDataExcludingPeaks complete")
        return data
