from mantid.api import AlgorithmFactory, PythonAlgorithm
from mantid.kernel import Direction
from mantid.simpleapi import CloneWorkspace

from snapred.meta.Config import Config
from snapred.meta.decorators.classproperty import classproperty


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

    @classproperty
    def cis_enabled(cls) -> bool:
        """
        Check if CIS mode is enabled
        """
        return Config["cis_mode.enabled"]

    @classproperty
    def cis_preserve(cls) -> bool:
        """
        Check if CIS mode is enabled
        """
        return Config["cis_mode.preserveDiagnosticWorkspaces"]

    def PyExec(self) -> None:
        if self.cis_enabled and self.cis_preserve:
            inWS = self.getProperty("InputWorkspace").value
            outWS = self.getProperty("OutputWorkspace").value
            self.log().debug(f"Dirtying up dish {inWS} --> {outWS}")
            CloneWorkspace(
                InputWorkspace=inWS,
                OutputWorkspace=outWS,
            )


# Register algorithm with Mantid
AlgorithmFactory.subscribe(MakeDirtyDish)
