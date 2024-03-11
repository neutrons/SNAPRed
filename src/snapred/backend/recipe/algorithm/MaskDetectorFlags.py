from typing import Dict

from mantid.api import (
    AlgorithmFactory,
    MatrixWorkspaceProperty,
    PropertyMode,
    PythonAlgorithm,
)
from mantid.dataobjects import MaskWorkspaceProperty
from mantid.kernel import Direction
from mantid.simpleapi import mtd


class MaskDetectorFlags(PythonAlgorithm):
    """
    Initialize detector flags from a mask workspace.
    """

    # This algorithm does what Mantid's `MaskDetectors` would be expected to do:
    #   * it initializes the detector mask flags from a mask workspace, and that's all;
    #   * it does NOT additionally _clear_ any masked spectra;
    #   * it does NOT move any masked detectors to group zero (for grouping workspaces).

    def category(self):
        return "SNAPRed Internal"

    def PyInit(self):
        # declare properties
        self.declareProperty(
            MaskWorkspaceProperty("MaskWorkspace", "", Direction.Input, PropertyMode.Mandatory),
            doc="Workspace containing the mask",
        )
        self.declareProperty(
            MatrixWorkspaceProperty("OutputWorkspace", "", Direction.InOut, PropertyMode.Mandatory),
            doc="The workspace for which to set the detector flags",
        )
        self.setRethrows(True)

    def validateInputs(self) -> Dict[str, str]:
        errors = {}

        self.maskWSName = self.getPropertyValue("MaskWorkspace")
        self.outputWSName = self.getPropertyValue("OutputWorkspace")

        self.maskWS = mtd[self.maskWSName]
        self.outputWS = mtd[self.outputWSName]

        sourceInstrument = self.maskWS.getInstrument()
        destInstrument = mtd[self.outputWSName].getInstrument()

        if sourceInstrument.getNumberDetectors(True) != destInstrument.getNumberDetectors(True):
            errors["MaskWorkspace"] = "Mask and output workspaces must have the same number of (non-monitor) pixels"

        if self.maskWS.getNumberHistograms() != destInstrument.getNumberDetectors(True):
            errors["MaskWorkspace"] = "Mask workspace must have one spectrum per (non-monitor) pixel"

        return errors

    def unbagGroceries(self):
        pass

    def PyExec(self) -> None:
        # Set the detector mask flags from the mask workspace values
        detectors = self.outputWS.detectorInfo()
        ids = detectors.detectorIDs()

        # Warning: <detector info>.indexOf(id_) != <mask workspace index of detectors excluding monitors>
        for id_ in ids:
            if detectors.isMonitor(detectors.indexOf(int(id_))):
                continue
            detectors.setMasked(int(id_), self.maskWS.isMasked(int(id_)))
        self.setPropertyValue("OutputWorkspace", self.outputWSName)


# Register algorithm with Mantid
AlgorithmFactory.subscribe(MaskDetectorFlags)
