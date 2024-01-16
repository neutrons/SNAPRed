from typing import Any, Dict

from mantid.api import AlgorithmManager

from snapred.backend.dao.ingredients import PeakIngredients as Ingredients
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.algorithm.SmoothDataExcludingPeaksAlgo import SmoothDataExcludingPeaksAlgo
from snapred.meta.decorators.Singleton import Singleton

logger = snapredLogger.getLogger(__name__)


@Singleton
class SmoothDataExcludingPeaksRecipe:
    smoothDataExcludingPeaksAlgoName: str = SmoothDataExcludingPeaksAlgo.__name__

    def __init__(self):
        pass

    def executeRecipe(self, ingredients: Ingredients):
        logger.info("Executing SmoothDataExcludingPeaksRecipe")
        data: Dict[str, Any] = {}

        algo = AlgorithmManager.create(self.smoothDataExcludingPeaksAlgoName)
        algo.setProperty("Ingredients", ingredients.json())
        try:
            data["result"] = algo.execute()
        except RuntimeError as e:
            errorString = str(e)
            raise Exception(errorString.split("\n")[0])
        logger.info("SmoothDataExcludingPeaks complete")
        return data
