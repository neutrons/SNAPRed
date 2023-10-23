import json
from typing import Dict, List, Tuple

import numpy as np
from mantid.api import AlgorithmFactory, PythonAlgorithm
from mantid.kernel import Direction, StringArrayProperty
from mantid.simpleapi import CloneWorkspace, mtd

from snapred.meta.Config import Config

class MakeDirtyDish(PythonAlgorithm):
    """
    Record a workspace in a state for the CIS to view later
    """

    def PyInit(self):
        # declare properties
        self.declareProperty("InputWorkspace", defaultValue="", direction=Direction.Input)  # noqa: F821
        self.declareProperty("OutputWorkspace", defaultValue="", direction=Direction.Output)  # noqa: F821
        self.setRethrows(True)
        self._CISmode: bool = Config["cis_mode"]

    def PyExec(self) -> None:
        inWS = self.getProperty("InputWorkspace").value
        outWS = self.getProperty("OutputWorkspace").value
        self.log().notice(f'Dirtying up dish {inWS} --> {outWS}')
        if self._CISmode:
            CloneWorkspace(
                InputWorkspace=inWS,
                OutputWorkspace=outWS,          
            )

# Register algorithm with Mantid
AlgorithmFactory.subscribe(MakeDirtyDish)
