from typing import Any, Dict

from mantid.api import AlgorithmManager
from snapred.backend.dao.FitMultiplePeaksIngredients import FitMultiplePeaksIngredients
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.algorithm.FitMultiplePeaksAlgorithm import (
    name as FitMultiplePeaksAlgorithm,
)
from snapred.meta.decorators.Singleton import Singleton

logger = snapredLogger.getLogger(__name__)


@Singleton
class FitMultiplePeaksRecipe:
    fitMultiplePeaksAlgorithmName: str = FitMultiplePeaksAlgorithm

    def __init__(self):
        pass

    def executeRecipe(self, fitMultiplePeakIngredients: FitMultiplePeaksIngredients):
        logger.info("Executing FitMultiplePeaksRecipe")
        data: Dict[str, Any] = {}

        algo = AlgorithmManager.create(self.fitMultiplePeaksAlgorithmName)
        algo.setProperty("FitMultiplePeaksIngredients", fitMultiplePeakIngredients.json())
        try:
            data["result"] = algo.execute()
        except RuntimeError as e:
            errorString = str(e)
            raise Exception(errorString.split("\n")[0])
        logger.info("FitMultiplePeaks complete")
        return data


