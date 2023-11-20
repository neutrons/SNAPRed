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

import numpy as np
from mantid.api import (
    AlgorithmFactory,
    MatrixWorkspaceProperty,
    PropertyMode,
    PythonAlgorithm,
)
from mantid.kernel import Direction
from scipy.interpolate import make_smoothing_spline, splev

from snapred.backend.dao.ingredients import SmoothDataExcludingPeaksIngredients as Ingredients
from snapred.backend.recipe.algorithm.DiffractionSpectrumWeightCalculator import DiffractionSpectrumWeightCalculator
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper

name = "SmoothDataExcludingPeaks"


# TODO: Rename so it matches filename
class SmoothDataExcludingPeaks(PythonAlgorithm):
    def PyInit(self):
        # declare properties
        self.declareProperty(
            MatrixWorkspaceProperty("InputWorkspace", "", Direction.Input, PropertyMode.Mandatory),
            doc="Workspace containing the peaks to be removed",
        )
        self.declareProperty("Ingredients", defaultValue="", direction=Direction.Input)
        self.declareProperty("OutputWorkspace", defaultValue="SmoothPeaks_out", direction=Direction.Output)
        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, name)

    def chopIngredients(self, ingredients: Ingredients):
        # chop off instrument state and crystal info
        self.lam = ingredients.smoothingParameter
        self.instrumentState = ingredients.instrumentState
        self.crystalInfo = ingredients.crystalInfo

    def unbagGroceries(self):
        self.inputWorkspaceName = self.getPropertyValue("InputWorkspace")
        self.weightWorkspaceName = datetime.now().ctime() + "_weight_ws"
        if self.getProperty("OutputWorkspace").isDefault:
            self.outputWorkspaceName = self.inputWorkspaceName + "_smoothed"  # TODO make a better name
            self.setPropertyValue("OutputWorkspace", self.outputWorkspaceName)
        else:
            self.outputWorkspaceName = self.getPropertyValue("OutputWorkspace")

    def PyExec(self):
        self.log().notice("Removing peaks and smoothing data")

        # load ingredients
        ingredients = Ingredients.parse_raw(self.getProperty("Ingredients").value)
        self.chopIngredients(ingredients)
        self.unbagGroceries()
        # call the diffraction spectrum weight calculator
        self.mantidSnapper.DiffractionSpectrumWeightCalculator(
            "Calculating spectrum weights...",
            InputWorkspace=self.inputWorkspaceName,
            InstrumentState=self.instrumentState.json(),
            CrystalInfo=self.crystalInfo.json(),
            WeightWorkspace=self.weightWorkspaceName,
        )
        # This is because its a histogram
        self.mantidSnapper.CloneWorkspace(
            "Cloning new workspace for smoothed spectrum data...",
            InputWorkspace=self.inputWorkspaceName,
            OutputWorkspace=self.outputWorkspaceName,
        )
        self.mantidSnapper.executeQueue()

        # get number of spectrum to iterate over
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
            smoothing_results = splev(xMidpoints, tck)

            outputWorkspace.setY(index, smoothing_results)
        self.mantidSnapper.WashDishes(
            "Cleaning up weight workspace...",
            Workspace=self.weightWorkspaceName,
        )
        self.mantidSnapper.executeQueue()


# Register algorithm with Mantid
AlgorithmFactory.subscribe(SmoothDataExcludingPeaks)
