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

import numpy as np
from mantid.api import (
    AlgorithmFactory,
    PythonAlgorithm,
    WorkspaceGroup,
    mtd,
)
from mantid.kernel import Direction
from scipy.interpolate import make_smoothing_spline, splev

from snapred.backend.dao.ingredients import SmoothDataExcludingPeaksIngredients as TheseIngredients
from snapred.backend.recipe.algorithm.DiffractionSpectrumWeightCalculator import DiffractionSpectrumWeightCalculator
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper


# TODO: Rename so it matches filename
class SmoothDataExcludingPeaksAlgo(PythonAlgorithm):
    def category(self):
        return "SNAPRed Sample Data"

    def PyInit(self):
        # declare properties
        self.declareProperty("InputWorkspace", defaultValue="", direction=Direction.Input)
        self.declareProperty("Ingredients", defaultValue="", direction=Direction.Input)
        self.declareProperty("OutputWorkspace", defaultValue="SmoothPeaks_out", direction=Direction.Output)
        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, __name__)

    def chopIngredients(self, ingredients: TheseIngredients):
        # chop off instrument state and crystal info
        self.lam = ingredients.smoothingParameter
        self.instrumentState = ingredients.instrumentState
        self.crystalInfo = ingredients.crystalInfo

    def PyExec(self):
        self.log().notice("Removing peaks and smoothing data")

        outputWorkspaceName = self.getProperty("OutputWorkspace").value

        # load ingredients
        ingredients = TheseIngredients(**json.loads(self.getProperty("Ingredients").value))
        self.chopIngredients(ingredients)

        # load workspace
        input_ws = self.getProperty("InputWorkspace").value
        ws = self.mantidSnapper.mtd[input_ws]

        # call the diffraction spectrum weight calculator
        weight_ws_name = "weight_ws"
        self.mantidSnapper.DiffractionSpectrumWeightCalculator(
            "Calculating spectrum weights...",
            InputWorkspace=input_ws,
            InstrumentState=self.instrumentState.json(),
            CrystalInfo=self.crystalInfo.json(),
            WeightWorkspace=weight_ws_name,
        )
        # This is because its a histogram
        self.mantidSnapper.CloneWorkspace(
            "Cloning new workspace for smoothed spectrum data...",
            InputWorkspace=input_ws,
            OutputWorkspace=outputWorkspaceName,
        )
        self.mantidSnapper.executeQueue()
        outputWorkspace = self.mantidSnapper.mtd[outputWorkspaceName]
        weights_ws = self.mantidSnapper.mtd[weight_ws_name]

        # get number of spectrum to iterate over

        numSpec = mtd[weight_ws_name].getNumberHistograms()

        # extract x & y data for csaps
        for index in range(numSpec):
            weightX = weights_ws.readX(index)
            x = ws.readX(index)
            w = weights_ws.readY(index)
            y = ws.readY(index)
            weightXMidpoints = (weightX[:-1] + weightX[1:]) / 2
            xMidpoints = (x[:-1] + x[1:]) / 2

            weightXMidpoints = weightXMidpoints[w != 0]
            y = y[w != 0]
            # Generate spline with purged dataset
            tck = make_smoothing_spline(weightXMidpoints, y, lam=self.lam)
            # fill in the removed data using the spline function and original datapoints
            smoothing_results = splev(xMidpoints, tck)

            outputWorkspace.setY(index, smoothing_results)

        self.mantidSnapper.WashDishes(
            "Cleaning up weight workspace...",
            Workspace=weight_ws_name,
        )
        self.mantidSnapper.executeQueue()

        self.setProperty("OutputWorkspace", outputWorkspaceName)
        return outputWorkspaceName


# Register algorithm with Mantid
AlgorithmFactory.subscribe(SmoothDataExcludingPeaksAlgo)
