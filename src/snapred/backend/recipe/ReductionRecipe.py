from snapred.meta.Singleton import Singleton
from snapred.backend.recipe.algorithm.DummyAlgo import DummyAlgo
from mantid.api import AlgorithmManager, AlgorithmObserver

@Singleton
class ReductionRecipe:
    reductionAlgoName = "DummyAlgo"

    def __init__(self):
        return

    def executeRecipe(self, reductionState):
        data = {}
        algo = AlgorithmManager.create(self.reductionAlgoName)
        # observer = AlgorithmObserver()
        # observer.observeFinish(algo)


        data["result"] = algo.execute()

        # import pdb; pdb.set_trace()
            

        # data["result"] = algo.getProperty("result")

        return data 
