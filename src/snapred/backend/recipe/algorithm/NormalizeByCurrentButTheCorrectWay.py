from mantid.api import AlgorithmFactory, MatrixWorkspaceProperty, PropertyMode, PythonAlgorithm
from mantid.kernel import Direction
from mantid.simpleapi import CloneWorkspace, NormaliseByCurrent, Scale, mtd


class NormalizeByCurrentButTheCorrectWay(PythonAlgorithm):
    """
    You want to normalise but current.
    But you don't want it to just crash if you already did.
    This makes sure it won't.
    aka NormaliseByProtonCharge
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
        self.declareProperty("NormalizeByMonitorCounts", 0.0, direction=Direction.Input)
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
            if not self.getProperty("NormalizeByMonitorCounts").isDefault:
                # pull the normalization factor from the workspace log
                normVal = 1.0 / self.getProperty("NormalizeByMonitorCounts").value

                if self.getPropertyValue("InputWorkspace") != self.getPropertyValue("OutputWorkspace"):
                    CloneWorkspace(
                        InputWorkspace=workspace,
                        OutputWorkspace=self.getPropertyValue("OutputWorkspace"),
                    )

                workspace = self.getPropertyValue("OutputWorkspace")

                workspace = Scale(InputWorkspace=workspace, Factor=normVal, Operation="Multiply")
                # add the normalization factor to the workspace log
                workspace.getRun().addProperty(
                    "NormalizationFactor",
                    normVal,
                    False,
                )
                self.setProperty("OutputWorkspace", workspace)
            else:
                NormaliseByCurrent(
                    InputWorkspace=workspace,
                    OutputWorkspace=self.getPropertyValue("OutputWorkspace"),
                    RecalculatePCharge=self.getProperty("RecalculatePCharge").value,
                )


# Register algorithm with Mantid
AlgorithmFactory.subscribe(NormalizeByCurrentButTheCorrectWay)
