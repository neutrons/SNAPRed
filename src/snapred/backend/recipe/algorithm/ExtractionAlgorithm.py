from mantid.api import AlgorithmFactory, PythonAlgorithm
from mantid.kernel import Direction

name = "ExtractionAlgorithm"


class ExtractionAlgorithm(PythonAlgorithm):
    def PyInit(self):
        # declare properties
        self.declareProperty("ExtractionIngredients", defaultValue="", direction=Direction.Input)  # noqa: F821
        self.declareProperty("OutputWorkspace", defaultValue="", direction=Direction.Output)
        self.setRethrows(True)

    def PyExec(self):
        # run the algo
        self.log().notice("Execution of extraction of calibration constants START!")


# Register algorithm with Mantid
AlgorithmFactory.subscribe(ExtractionAlgorithm)
