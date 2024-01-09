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
from datetime import datetime
from typing import Dict

import numpy as np
from mantid.api import (
    AlgorithmFactory,
    MatrixWorkspaceProperty,
    PropertyMode,
    PythonAlgorithm,
    IEventWorkspace
)

from mantid.kernel import Direction
from scipy.interpolate import make_smoothing_spline

from snapred.backend.dao.ingredients import SmoothDataExcludingPeaksIngredients as Ingredients
from snapred.backend.recipe.algorithm.DiffractionSpectrumWeightCalculator import DiffractionSpectrumWeightCalculator
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper


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
            MatrixWorkspaceProperty("OutputWorkspace", "", Direction.Output, PropertyMode.Optional),
            doc="Workspace with removed peaks",
        )
        self.declareProperty("Ingredients", defaultValue="", direction=Direction.Input)
        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, __name__)

    def chopIngredients(self, ingredients: Ingredients):
        # chop off instrument state and crystal info
        self.lam = ingredients.smoothingParameter
        self.instrumentState = ingredients.instrumentState
        self.crystalInfo = ingredients.crystalInfo

    def unbagGroceries(self):
        self.inputWorkspaceName = self.getPropertyValue("InputWorkspace")
        self.weightWorkspaceName = datetime.now().ctime() + "_weight_ws"
        self.outputWorkspaceName = self.getPropertyValue("OutputWorkspace")

    def validateInputs(self) -> Dict[str, str]:
        errors = {}
        if self.getProperty("OutputWorkspace").isDefault:
            errors["Outputworkspace"] = "must specify output workspace with smoothed peaks"
        return errors

    def PyExec(self):
        self.log().notice("Removing peaks and smoothing data")

        # load ingredients
        ingredients = Ingredients.parse_raw(self.getProperty("Ingredients").value)
        self.chopIngredients(ingredients)
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
            InstrumentState=self.instrumentState.json(),
            CrystalInfo=self.crystalInfo.json(),
            WeightWorkspace=self.weightWorkspaceName,
        )
        self.mantidSnapper.executeQueue()

        # get handles to the workspaces
        inputWorkspace = self.mantidSnapper.mtd[self.inputWorkspaceName]
        outputWorkspace = self.mantidSnapper.mtd[self.outputWorkspaceName]
        weight_ws = self.mantidSnapper.mtd[self.weightWorkspaceName]

        numSpec = weight_ws.getNumberHistograms()
        # extract x & y data for csaps
        for index in range(numSpec):
            x = inputWorkspace.readX(index)
            y = inputWorkspace.readY(index)

            weightX = weight_ws.readX(index)
            weightY = weight_ws.readY(index)

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


# Register algorithm with Mantid
AlgorithmFactory.subscribe(SmoothDataExcludingPeaksAlgo)
