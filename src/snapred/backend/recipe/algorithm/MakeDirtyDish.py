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
        self._CISmode: bool = Config["cis_mode"]

    def PyExec(self) -> None:
        inWS = self.getProperty("InputWorkspace").value
        outWS = self.getProperty("OutputWorkspace").value
        self.log().notice(f"Dirtying up dish {inWS} --> {outWS}")
        if self._CISmode:
            CloneWorkspace(
                InputWorkspace=inWS,
                OutputWorkspace=outWS,
            )


# Register algorithm with Mantid
AlgorithmFactory.subscribe(MakeDirtyDish)
