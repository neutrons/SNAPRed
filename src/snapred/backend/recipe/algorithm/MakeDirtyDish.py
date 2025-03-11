from mantid.api import AlgorithmFactory, PythonAlgorithm
from mantid.kernel import Direction
from mantid.simpleapi import CloneWorkspace

from snapred.meta.Config import Config


class MakeDirtyDish(PythonAlgorithm):
    """
    Record a workspace in a state for the CIS to view later
    """

    def category(self):
        return "SNAPRed Internal"

    def PyInit(self):
        # declare properties
        self.declareProperty("InputWorkspace", defaultValue="", direction=Direction.Input)  # noqa: F821
        self.declareProperty("OutputWorkspace", defaultValue="", direction=Direction.Output)  # noqa: F821
        self.setRethrows(True)

        cisModeConfig = Config["cis_mode"]
        self.enabled: bool = cisModeConfig.get("enabled")
        self.preserve: bool = cisModeConfig.get("preserveDiagnosticWorkspaces")

        if self.enabled != self.preserve:
            self.log().warning(
                f"Mismatch in config: cis_mode.enabled={self.enabled}, "
                f"cis_mode.preserveDiagnosticWorkspaces={self.preserve}."
            )

    def PyExec(self) -> None:
        if self.enabled and self.preserve:
            inWS = self.getProperty("InputWorkspace").value
            outWS = self.getProperty("OutputWorkspace").value
            self.log().debug(f"Dirtying up dish {inWS} --> {outWS}")
            CloneWorkspace(
                InputWorkspace=inWS,
                OutputWorkspace=outWS,
            )


# Register algorithm with Mantid
AlgorithmFactory.subscribe(MakeDirtyDish)
