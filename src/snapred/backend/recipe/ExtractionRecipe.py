from typing import Any, Dict

from mantid.api import AlgorithmManager

from snapred.backend.dao.ExtractionIngredients import ExtractionIngredients
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.algorithm.ExtractionAlgorithm import (
    name as ExtractionAlgorithm,
)
from snapred.meta.Singleton import Singleton

logger = snapredLogger.getLogger(__name__)


@Singleton
class ExtractionRecipe:
    extractionAlgorithmName: str = ExtractionAlgorithm

    def __init__(self):
        pass

    def executeRecipe(self, ingredients: ExtractionIngredients) -> Dict[str, Any]:
        logger.info("Executing recipe for runId: %s" % ingredients.runConfig.runNumber)
        data: Dict[str, Any] = {}

        algo = AlgorithmManager.create(self.extractionAlgorithmName)
        algo.setProperty("ExtractionIngredients", ingredients.json())

        try:
            data["result"] = algo.execute()
        except RuntimeError as e:
            errorString = str(e)
            raise Exception(errorString.split("\n")[0])
        logger.info("Finished executing recipe for runId: %s" % ingredients.runConfig.runNumber)
        return data
