from mantid.api import AlgorithmFactory, PythonAlgorithm

from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper


class Utensils(PythonAlgorithm):
    """
    Empty Algorithm used as a workaround for algorithms requiring progress reporting.
    """
    def __init__(self, non_queued_execution=False):
        self._non_queued_execution = non_queued_execution
        super().__init__()
        
    def category(self):
        return "SNAPRed Internal"

    def PyInit(self):
        self.mantidSnapper = MantidSnapper(
            self,
            __name__,
            non_queued_execution=self._non_queued_execution
        )

    def PyExec(self) -> None:
        pass


# Register algorithm with Mantid
AlgorithmFactory.subscribe(Utensils)
