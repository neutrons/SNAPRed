from mantid.api import AlgorithmFactory, PythonAlgorithm

from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper


class Utensils(PythonAlgorithm):
    """
    Empty Algo used as a workaround for Progress requiring an algo.
    """

    def category(self):
        return "SNAPRed Internal"

    def PyInit(self):
        self.mantidSnapper = MantidSnapper(self, __name__)

    def PyExec(self) -> None:
        pass


# Register algorithm with Mantid
AlgorithmFactory.subscribe(Utensils)
