import numpy as np
from mantid.api import (
    AlgorithmFactory,
    MatrixWorkspaceProperty,
    PropertyMode,
    PythonAlgorithm,
)
from mantid.kernel import Direction, PropertyManagerProperty


class OffsetStatistics(PythonAlgorithm):
    """
    Given an Offsets workspace, return statistical information
    of the offsets.  Can be used to help gauge convergence of
    the diffcal process.
    """

    OFFSETWKSPPROP = "OffsetsWorkspace"

    def category(self):
        return "SNAPRed Diffraction Calibration"

    def PyInit(self):
        # declare properties
        self.declareProperty(
            MatrixWorkspaceProperty(self.OFFSETWKSPPROP, "", Direction.Input, PropertyMode.Mandatory),
            doc="Workspace containing the TOF neutron data",
        )
        self.declareProperty(PropertyManagerProperty("Data", dict(), direction=Direction.Output))
        self.setRethrows(True)
        self._counts = 0

    def PyExec(self) -> None:
        self.log().notice("Calculate the statistics of an offsets workspace")
        offsets = list(self.getProperty(self.OFFSETWKSPPROP).value.extractY().ravel())
        absOffsets = [abs(offset) for offset in offsets]

        data = {}
        # (Warning: `median(abs(offsets))` vs. `abs(median(offsets)`: the former introduces oscillation artifacts.)
        data["medianOffset"] = float(abs(np.median(offsets)))
        data["meanOffset"] = float(abs(np.mean(offsets)))
        data["minOffset"] = float(np.min(offsets))
        data["maxOffset"] = float(np.max(offsets))
        data["maxAbsoluteOffset"] = float(np.max(absOffsets))
        data["minAbsoluteOFfset"] = float(np.min(absOffsets))

        self.setProperty("Data", data)


# Register algorithm with Mantid
AlgorithmFactory.subscribe(OffsetStatistics)
