from snapred.meta.Singleton import Singleton
from snapred.backend.recipe.algorithm.DummyAlgo import DummyAlgo
from snapred.backend.log.logger import snapredLogger

from mantid.api import AlgorithmManager, AlgorithmObserver

logger = snapredLogger.getLogger(__name__)

# TODO: Need progress reporting but should this be an algo? algos are instantiated, recipes are stateless
@Singleton
class ReductionRecipe:
    reductionAlgoName = "DummyAlgo"

    def __init__(self):
        return

    def executeRecipe(self, reductionState):
        logger.info("Executing recipe for runId: %s" % reductionState.runId)
        data = {}
        algo = AlgorithmManager.create(self.reductionAlgoName)

        data["result"] = algo.execute()

        logger.info("Finished executing recipe for runId: %s" % reductionState.runId)
        return data 

    def executeRecipe(self, reductionRequest):
        return