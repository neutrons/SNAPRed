from mantid.api import AlgorithmFactory, PythonAlgorithm


class Utensils(PythonAlgorithm):
    """
    Empty Algo used as a workaround for Progress requiring an algo.
    """

    def category(self):
        return "SNAPRed Internal"

    def PyInit(self):
        pass

    def PyExec(self) -> None:
        pass


# Register algorithm with Mantid
AlgorithmFactory.subscribe(Utensils)
