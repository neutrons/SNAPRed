from mantid.api import AlgorithmFactory, PythonAlgorithm
from mantid.kernel import Direction

name = "ExtractionAlgorithm"


class ExtractionAlgorithm(PythonAlgorithm):
    def PyInit(self):
        # declare properties
        self.declareProperty("ExtractionIngredients", defaultValue="", direction=Direction.Input)  # noqa: F821

    def PyExec(self):
        # run the algo
        # self.log().notice("exec extract diffractometer constants, empty Algo")
        pass


# Register algorithm with Mantid
AlgorithmFactory.subscribe(ExtractionAlgorithm)
