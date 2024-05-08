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

import json
from datetime import datetime
from typing import Dict

import numpy as np
from mantid.api import AlgorithmFactory, IEventWorkspace, MatrixWorkspaceProperty, PropertyMode, PythonAlgorithm
from mantid.kernel import Direction
from scipy.interpolate import make_smoothing_spline

from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper

logger = snapredLogger.getLogger(__name__)


# TODO: Rename so it matches filename
class SmoothDataExcludingPeaksAlgo(PythonAlgorithm):
    def category(self):
        return "SNAPRed Data Processing"

    def PyInit(self):
        # declare properties
        self.declareProperty(
            MatrixWorkspaceProperty("InputWorkspace", "", Direction.Input, PropertyMode.Mandatory),
            doc="Workspace containing the peaks to be removed",
        )
        self.declareProperty(
            MatrixWorkspaceProperty("OutputWorkspace", "", Direction.Output, PropertyMode.Mandatory),
            doc="Workspace with removed peaks",
        )
        self.declareProperty("DetectorPeaks", defaultValue="", direction=Direction.Input)
        self.declareProperty("SmoothingParameter", defaultValue=-1.0, direction=Direction.Input)
        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, __name__)

    def chopIngredients(self, ingredients):  # noqa ARG002
        # NOTE there are no ingredients
        pass

    def unbagGroceries(self):
        self.inputWorkspaceName = self.getPropertyValue("InputWorkspace")
        self.weightWorkspaceName = datetime.now().ctime() + "_weight_ws"
        self.outputWorkspaceName = self.getPropertyValue("OutputWorkspace")

    def validateInputs(self) -> Dict[str, str]:
        errors = {}
        return errors

    def PyExec(self):
        self.log().notice("Removing peaks and smoothing data")
        self.lam = float(self.getPropertyValue("SmoothingParameter"))
        self.unbagGroceries()

        # copy input to make output workspace
        if self.inputWorkspaceName != self.outputWorkspaceName:
            self.mantidSnapper.CloneWorkspace(
                "Cloning new workspace for smoothed spectrum data...",
                InputWorkspace=self.inputWorkspaceName,
                OutputWorkspace=self.outputWorkspaceName,
            )
        # call the diffraction spectrum weight calculator
        self.mantidSnapper.DiffractionSpectrumWeightCalculator(
            "Calculating spectrum weights...",
            InputWorkspace=self.outputWorkspaceName,
            DetectorPeaks=self.getPropertyValue("DetectorPeaks"),
            WeightWorkspace=self.weightWorkspaceName,
        )
        self.mantidSnapper.executeQueue()

        # get handles to the workspaces
        inputWorkspace = self.mantidSnapper.mtd[self.inputWorkspaceName]
        outputWorkspace = self.mantidSnapper.mtd[self.outputWorkspaceName]
        weightWorkspace = self.mantidSnapper.mtd[self.weightWorkspaceName]

        numSpec = weightWorkspace.getNumberHistograms()

        for index in range(numSpec):
            x = inputWorkspace.readX(index)
            y = inputWorkspace.readY(index)

            weightX = weightWorkspace.readX(index)
            weightY = weightWorkspace.readY(index)

            weightXMidpoints = (weightX[:-1] + weightX[1:]) / 2
            xMidpoints = (x[:-1] + x[1:]) / 2

            weightXMidpoints = weightXMidpoints[weightY != 0]
            y = y[weightY != 0]
            # Generate spline with purged dataset
            tck = make_smoothing_spline(weightXMidpoints, y, lam=self.lam)
            # fill in the removed data using the spline function and original datapoints
            smoothing_results = tck(xMidpoints, extrapolate=False)
            outputWorkspace.setY(index, smoothing_results)

        self.mantidSnapper.WashDishes(
            "Cleaning up weight workspace...",
            Workspace=self.weightWorkspaceName,
        )
        self.mantidSnapper.executeQueue()
        self.setProperty("OutputWorkspace", outputWorkspace)


AlgorithmFactory.subscribe(SmoothDataExcludingPeaksAlgo)
