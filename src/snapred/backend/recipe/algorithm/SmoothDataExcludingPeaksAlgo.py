# Logic notes:
"""
load nexus
num spe = num groups (instrument state) --> consitant with workspace
state & crystal info --> consitant with workspace
call diffraction spectrum peak predictor --> to get peaks (num groups)
call diffraction spectrum weight calc (send one spectra only)
extract x and y data from a workspace (numpy arrray)
implement csaps
create new workspace with csaps data
"""

from typing import Dict

from mantid.api import (
    IEventWorkspace,
    MatrixWorkspaceProperty,
    PropertyMode,
    PythonAlgorithm,
    WorkspaceUnitValidator,
)
from mantid.kernel import Direction, FloatBoundedValidator
from mantid.kernel import ULongLongPropertyWithValue as PointerProperty
from mantid.simpleapi import (
    CloneWorkspace,
    ConvertToMatrixWorkspace,
    DiffractionSpectrumWeightCalculator,
    WashDishes,
    mtd,
)
from scipy.interpolate import make_smoothing_spline

from snapred.backend.log.logger import snapredLogger

logger = snapredLogger.getLogger(__name__)


class SmoothDataExcludingPeaksAlgo(PythonAlgorithm):
    def category(self):
        return "SNAPRed Data Processing"

    def PyInit(self):
        # declare properties
        self.declareProperty(
            MatrixWorkspaceProperty(
                "InputWorkspace",
                defaultValue="",
                direction=Direction.Input,
                optional=PropertyMode.Mandatory,
                validator=WorkspaceUnitValidator("dSpacing"),
            ),
            doc="Workspace containing the peaks to be removed",
        )
        self.declareProperty(
            MatrixWorkspaceProperty(
                "OutputWorkspace",
                defaultValue="",
                direction=Direction.Output,
                optional=PropertyMode.Mandatory,
                validator=WorkspaceUnitValidator("dSpacing"),
            ),
            doc="Histogram Workspace with removed peaks",
        )
        self.declareProperty(
            PointerProperty("DetectorPeaks", id(None)),
            "The memory address pointing to the list of grouped peaks.",
        )
        self.declareProperty(
            "SmoothingParameter",
            defaultValue=-1.0,
            validator=FloatBoundedValidator(lower=0.0),
        )
        self.setRethrows(True)

    def chopIngredients(self, ingredients):  # noqa ARG002
        # NOTE there are no ingredients
        pass

    def unbagGroceries(self):
        self.inputWorkspaceName = self.getPropertyValue("InputWorkspace")
        self.outputWorkspaceName = self.getPropertyValue("OutputWorkspace")

    def validateInputs(self) -> Dict[str, str]:
        errors = {}
        return errors

    def PyExec(self):
        self.log().notice("Removing peaks and smoothing data")
        self.lam = self.getProperty("SmoothingParameter").value
        self.unbagGroceries()

        # copy input to make output workspace
        if self.inputWorkspaceName != self.outputWorkspaceName:
            CloneWorkspace(
                InputWorkspace=self.inputWorkspaceName,
                OutputWorkspace=self.outputWorkspaceName,
            )

        # check if input is an event workspace
        if isinstance(mtd[self.inputWorkspaceName], IEventWorkspace):
            # convert it to a histogram
            ConvertToMatrixWorkspace(
                InputWorkspace=self.outputWorkspaceName,
                OutputWorkspace=self.outputWorkspaceName,
            )

        # call the diffraction spectrum weight calculator
        weightWSname = mtd.unique_name(prefix="_weight_")
        DiffractionSpectrumWeightCalculator(
            InputWorkspace=self.outputWorkspaceName,
            DetectorPeaks=self.getProperty("DetectorPeaks").value,
            WeightWorkspace=weightWSname,
        )

        # get handles to the workspaces
        inputWorkspace = mtd[self.inputWorkspaceName]
        outputWorkspace = mtd[self.outputWorkspaceName]
        weightWorkspace = mtd[weightWSname]

        numSpec = weightWorkspace.getNumberHistograms()

        for index in range(numSpec):
            # get the weights
            weightX = weightWorkspace.readX(index)
            weightY = weightWorkspace.readY(index)

            # use the weight midpoint
            weightXMidpoints = (weightX[:-1] + weightX[1:]) / 2
            weightXMidpoints = weightXMidpoints[weightY != 0]

            # get the data for background and remove peaks
            y = inputWorkspace.readY(index).copy()
            y = y[weightY != 0]
            x = inputWorkspace.readX(index)
            xMidpoints = (x[:-1] + x[1:]) / 2.0

            # throw an exception if y or weightXMidpoints are empty
            if len(y) == 0 or len(weightXMidpoints) == 0:
                raise ValueError("No data in the workspace, all data removed by peak removal.")

            # Generate spline with purged dataset
            tck = make_smoothing_spline(weightXMidpoints, y, lam=self.lam)
            # fill in the removed data using the spline function and original datapoints
            smoothing_results = tck(xMidpoints, extrapolate=False)
            smoothing_results[smoothing_results < 0] = 0
            outputWorkspace.setY(index, smoothing_results)

        # cleanup
        WashDishes(weightWSname)

        self.setProperty("OutputWorkspace", outputWorkspace)
