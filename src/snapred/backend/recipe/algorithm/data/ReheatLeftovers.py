from mantid.api import (
    AlgorithmFactory,
    FileAction,
    FileProperty,
    PythonAlgorithm,
)
from mantid.kernel import Direction

from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper


class ReheatLeftovers(PythonAlgorithm):
    """
    Load ragged workspaces with a small number of histograms (< 20?) from a file.
    """

    def category(self):
        return "SNAPRed Internal"

    def PyInit(self):
        # declare properties
        self.declareProperty(
            FileProperty(
                "Filename",
                defaultValue="",
                action=FileAction.Load,
                extensions=["nxs.h5"],
                direction=Direction.Input,
            ),
            doc="Path to file to be loaded",
        )
        self.declareProperty(
            "OutputWorkspace", defaultValue="", direction=Direction.Output, doc="Workspace to be loaded"
        )
        self.mantidSnapper = MantidSnapper(self, __name__)

    def validate(self):
        pass

    def unbagGroceries(self):
        self.outputWSName = self.getPropertyValue("OutputWorkspace")
        self.filename = self.getPropertyValue("Filename")

    def PyExec(self) -> None:
        self.unbagGroceries()
        self.validate()

        self.mantidSnapper.LoadNexus(
            "Load the saved workspaces",
            Filename=self.filename,
            OutputWorkspace=self.outputWSName,
        )
        self.mantidSnapper.executeQueue()
        self.setProperty("OutputWorkspace", self.outputWSName)


# Register algorithm with Mantid
AlgorithmFactory.subscribe(ReheatLeftovers)
