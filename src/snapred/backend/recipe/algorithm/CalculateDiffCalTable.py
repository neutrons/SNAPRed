import json
from typing import Dict, List, Tuple

import numpy as np
from mantid.api import (
    AlgorithmFactory,
    ITableWorkspaceProperty,
    MatrixWorkspaceProperty,
    PropertyMode,
    PythonAlgorithm,
)
from mantid.kernel import Direction, StringListValidator
from mantid.simpleapi import (
    CalculateDIFC,
    CreateEmptyTableWorkspace,
    DeleteWorkspace,
    _create_algorithm_function,
    mtd,
)


class CalculateDiffCalTable(PythonAlgorithm):
    """
    Record a workspace in a state for the CIS to view later
    """

    def category(self):
        return "SNAPRed Diffraction Calibration"

    def PyInit(self):
        # declare properties
        self.declareProperty(
            MatrixWorkspaceProperty("InputWorkspace", "", Direction.Input, PropertyMode.Mandatory),
            doc="Workspace containing the instrument definition",
        )
        self.declareProperty(
            ITableWorkspaceProperty("CalibrationTable", "", Direction.Output, PropertyMode.Optional),
            doc="The resulting calibration table",
        )
        validOffsetModes = ["Signed", "Relative", "Absolute"]
        self.declareProperty("OffsetMode", "Signed", StringListValidator(validOffsetModes), direction=Direction.Input)
        self.declareProperty("BinWidth", 0.0, direction=Direction.Input)
        self.setRethrows(True)

    def PyExec(self) -> None:
        """
        Use the instrument in the input workspace to create an initial DIFC table
        Because the created DIFC is inside a matrix workspace, it must
        be manually loaded into a table workspace
        """
        self.log().notice("Creating DIFC table")

        # prepare initial diffraction calibration workspace
        tmpDifc = mtd.unique_name(prefix="_tmp_")
        CalculateDIFC(
            InputWorkspace=self.getPropertyValue("InputWorkspace"),
            OutputWorkspace=tmpDifc,
            OffsetMode=self.getPropertyValue("OffsetMode"),
            BinWidth=abs(self.getProperty("BinWidth").value),
        )
        tmpDifcWS = mtd[tmpDifc]
        difcs = [float(x) for x in tmpDifcWS.extractY()]

        # convert the calibration workspace into a calibration table
        DIFCtable = CreateEmptyTableWorkspace(
            OutputWorkspace=self.getPropertyValue("CalibrationTable"),
        )
        DIFCtable.addColumn(type="int", name="detid", plottype=6)
        DIFCtable.addColumn(type="double", name="difc", plottype=6)
        DIFCtable.addColumn(type="double", name="difa", plottype=6)
        DIFCtable.addColumn(type="double", name="tzero", plottype=6)

        for wkspIndx, difc in enumerate(difcs):
            detids = tmpDifcWS.getSpectrum(wkspIndx).getDetectorIDs()
            for detid in detids:
                DIFCtable.addRow(
                    {
                        "detid": int(detid),
                        "difc": float(difc),
                        "difa": 0.0,
                        "tzero": 0.0,
                    }
                )
        DeleteWorkspace(
            Workspace=tmpDifc,
        )
        self.setProperty("CalibrationTable", DIFCtable)


# Register algorithm with Mantid
AlgorithmFactory.subscribe(CalculateDiffCalTable)

# Puts function in simpleapi globals
algo = CalculateDiffCalTable()
algo.initialize()
_create_algorithm_function("CalculateDiffCalTable", 1, algo)
