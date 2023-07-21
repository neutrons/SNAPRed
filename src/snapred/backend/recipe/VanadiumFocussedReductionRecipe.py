from typing import Any, Dict

from mantid.api import AlgorithmManager

from snapred.backend.dao.ReductionIngredients import ReductionIngredients
from snapred.backend.dao.SmoothDataExcludingPeaksIngredients import SmoothDataExcludingPeaksIngredients
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.algorithm.VanadiumFocussedReductionAlgorithm import (
    name as VanadiumFocussedReductionAlgorithm,
)
from snapred.meta.decorators.Singleton import Singleton

logger = snapredLogger.getLogger(__name__)


@Singleton
class VanadiumFocussedReductionRecipe:
    reductionAlgorithmName: str = VanadiumFocussedReductionAlgorithm

    def __init__(self):
        pass

    def executeRecipe(
        self, reductionIngredients: ReductionIngredients, smoothIngredients: SmoothDataExcludingPeaksIngredients
    ):
        logger.info("Executing recipe for runId: %s" % reductionIngredients.runConfig.runNumber)
        data: Dict[str, Any] = {}

        algo = AlgorithmManager.create(self.reductionAlgorithmName)
        algo.setProperty("ReductionIngredients", reductionIngredients.json())
        algo.setProperty("SmoothDataIngredients", smoothIngredients.json())
        try:
            data["result"] = algo.execute()
        except RuntimeError as e:
            errorString = str(e)
            raise Exception(errorString.split("\n")[0])
        logger.info("Finished executing recipe for runId: %s" % reductionIngredients.runConfig.runNumber)
        return data
