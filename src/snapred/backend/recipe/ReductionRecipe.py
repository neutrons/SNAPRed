from snapred.meta.Singleton import Singleton
from snapred.backend.recipe.algorithm.ReductionAlgorithm import name as ReductionAlgorithm
from snapred.backend.log.logger import snapredLogger

from mantid.api import AlgorithmManager

logger = snapredLogger.getLogger(__name__)

# TODO: Need progress reporting but should this be an algo? algos are instantiated, recipes are stateless
@Singleton
class ReductionRecipe:
    reductionAlgorithmName = ReductionAlgorithm

    def __init__(self):
        return

    def executeRecipe(self, reductionIngredients):
        logger.info("Executing recipe for runId: %s" % reductionIngredients.reductionState.runId)
        data = {}
        algo = AlgorithmManager.create(self.reductionAlgorithmName)
        algo.setProperty("ReductionIngredients", reductionIngredients)

        data["result"] = algo.execute()

        logger.info("Finished executing recipe for runId: %s" % reductionIngredients.reductionState.runId)
        return data 