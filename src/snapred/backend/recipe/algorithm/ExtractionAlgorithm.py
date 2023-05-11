from mantid.api import AlgorithmFactory, PythonAlgorithm
from mantid.kernel import Direction

# from mantid.kernel import *

name = "ExtractionAlgorithm"


class ExtractionAlgorithm(PythonAlgorithm):
    def PyInit(self):
        # declare properties
        self.declareProperty("ExtractionIngredients", defaultValue="", direction=Direction.Input)  # noqa: F821

    def PyExec(self):
        # run the algo
        self.log().notice("exec extract diffractometer constants, empty Algo")


# Register algorithm with Mantid
AlgorithmFactory.subscribe(ExtractionAlgorithm)
