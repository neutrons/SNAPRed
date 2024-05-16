from mantid.api import AlgorithmFactory, MatrixWorkspaceProperty, PropertyMode, PythonAlgorithm
from mantid.kernel import Direction
from mantid.simpleapi import CloneWorkspace, NormaliseByCurrent, mtd


class NormalizeByCurrentButTheCorrectWay(PythonAlgorithm):
    """
    You want to normalise but current.
    But you don't want it to just crash if you already did.
    This makes sure it won't.
    """

    def category(self):
        return "SNAPRed Reduction"

    def PyInit(self):
        # declare properties
        self.declareProperty(
            MatrixWorkspaceProperty("InputWorkspace", "", Direction.Input, PropertyMode.Mandatory),
            doc="Name of the input workspace",
        )
        self.declareProperty(
            MatrixWorkspaceProperty("OutputWorkspace", "", Direction.Output, PropertyMode.Mandatory),
            doc="Name of the output workspace",
        )
        self.declareProperty("RecalculatePCharge", False, direction=Direction.Input)  # noqa: F821
        self.setRethrows(True)

    def PyExec(self):
        self.log().notice("Normalizing by current, but properly")
        workspace = self.getPropertyValue("InputWorkspace")
        if mtd[workspace].mutableRun().hasProperty("NormalizationFactor"):
            CloneWorkspace(
                InputWorkspace=workspace,
                OutputWorkspace=self.getPropertyValue("OutputWorkspace"),
            )
        else:
            NormaliseByCurrent(
                InputWorkspace=workspace,
                OutputWorkspace=self.getPropertyValue("OutputWorkspace"),
                RecalculatePCharge=self.getProperty("RecalculatePCharge").value,
            )


# Register algorithm with Mantid
AlgorithmFactory.subscribe(NormalizeByCurrentButTheCorrectWay)
