from typing import Any, Dict

from snapred.backend.dao.ExtractionIngredients import ExtractionIngredients
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.algorithm.IngestCrystallographicInfo import (
    name as IngestCrystallographicInfo,
)
from snapred.meta.Singleton import Singleton

logger = snapredLogger.getLogger(__name__)


@Singleton
class IngestCrystallographicInfoRecipe:
    ingestionAlgorithmName: str = IngestCrystallographicInfo

    def __init__(self):
        pass

    def executeRecipe(self, ingredients: ExtractionIngredients) -> Dict[str, Any]:
        logger.info("Executing recipe for runId: %s" % ingredients.runConfig.runNumber)
        data: Dict[str, Any] = {}

        # algo = AlgorithmManager.create(self.extractionAlgorithmName)
        # algo.setProperty("ExtractionIngredients", ingredients.json())

        # try:
        #     data["result"] = algo.execute()
        # except RuntimeError as e:
        #     errorString = str(e)
        #     raise Exception(errorString.split("\n")[0])
        # logger.info("Finished executing recipe for runId: %s" % ingredients.runConfig.runNumber)
        return data
