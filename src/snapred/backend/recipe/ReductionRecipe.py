from snapred.meta.Singleton import Singleton
from snapred.backend.dao.ReductionIngredients import ReductionIngredients
from snapred.backend.recipe.algorithm.ReductionAlgorithm import (
    name as ReductionAlgorithm,
)
from snapred.backend.recipe.algorithm.AlignAndFocusReductionAlgorithm import (
    name as AlignAndFocusReductionAlgorithm,
)
from snapred.backend.log.logger import snapredLogger

from typing import Dict, Any

from mantid.api import AlgorithmManager

logger = snapredLogger.getLogger(__name__)


@Singleton
class ReductionRecipe:
    reductionAlgorithmName: str = ReductionAlgorithm

    def __init__(self):
        pass

    def executeRecipe(self, reductionIngredients: ReductionIngredients):
        logger.info(
            "Executing recipe for runId: %s" % reductionIngredients.runConfig.runNumber
        )
        data: Dict[str, Any] = {}

        algo = AlgorithmManager.create(self.reductionAlgorithmName)
        algo.setProperty("ReductionIngredients", reductionIngredients.json())

        try:
            data["result"] = algo.execute()
        except RuntimeError as e:
            errorString = str(e)
            raise Exception(errorString.split("\n")[0])
        logger.info("Finished executing recipe for runId: %s" % reductionIngredients.runConfig.runNumber)
        return data 
