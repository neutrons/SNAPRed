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

from snapred.backend.dao.ingredients import PeakIngredients as Ingredients
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.algorithm.DiffractionSpectrumWeightCalculator import DiffractionSpectrumWeightCalculator
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
        self.declareProperty("DetectorPeakIngredients", defaultValue="", direction=Direction.Input)
        self.declareProperty("DetectorPeaks", defaultValue="", direction=Direction.Input)
        self.declareProperty("SmoothingParameter", defaultValue="", direction=Direction.Input)
        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, __name__)

    def chopIngredients(self, ingredients: Ingredients):
        if self.getProperty("SmoothingParameter").isDefault:
            self.lam = ingredients.smoothingParameter
        else:
            self.lam = float(self.getPropertyValue("SmoothingParameter"))

    def unbagGroceries(self):
        self.inputWorkspaceName = self.getPropertyValue("InputWorkspace")
        self.weightWorkspaceName = datetime.now().ctime() + "_weight_ws"
        self.outputWorkspaceName = self.getPropertyValue("OutputWorkspace")

    def validateInputs(self) -> Dict[str, str]:
        errors = {}
        # validate source of peak lists
        waysToGetPeaks = ["DetectorPeaks", "DetectorPeakIngredients"]
        definedWaysToGetPeaks = [x for x in waysToGetPeaks if not self.getProperty(x).isDefault]
        if len(definedWaysToGetPeaks) == 0:
            msg = "Purse peaks requires either a list of peaks, or ingredients to detect peaks"
            errors["DetectorPeaks"] = msg
            errors["DetectorPeakIngredients"] = msg
        elif len(definedWaysToGetPeaks) == 2:
            logger.warn(
                """Both a list of detector peaks and ingredients were given;
                the list will be used and ingredients ignored"""
            )
        # validate sources of smoothing parameter
        ingredientSmoothParam = json.loads(self.getPropertyValue("DetectorPeakIngredients")).get("smoothingParameter")
        specifiedIngredients = ingredientSmoothParam is not None
        specifiedProperties = not self.getProperty("SmoothingParameter").isDefault
        if not specifiedIngredients and not specifiedProperties:
            msg = "You must specify smoothing parameter through either ingredients or property"
            errors["DetectorPeakIngredients"] = msg
            errors["SmoothingParameter"] = msg
        if specifiedIngredients and specifiedProperties:
            logger.warn(
                """Smoothing parameter was specified through both property and ingredients;
                The value in ingredients will be ignored."""
            )
        return errors

    def PyExec(self):
        self.log().notice("Removing peaks and smoothing data")

        # load ingredients
        ingredients = Ingredients.parse_raw(self.getPropertyValue("DetectorPeakIngredients"))
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
            DetectorPeaks=self.getPropertyValue("DetectorPeaks"),
            DetectorPeakIngredients=ingredients.json(),
            WeightWorkspace=self.weightWorkspaceName,
        )
        self.mantidSnapper.executeQueue()

        # get handles to the workspaces
        inputWorkspace = self.mantidSnapper.mtd[self.inputWorkspaceName]
        outputWorkspace = self.mantidSnapper.mtd[self.outputWorkspaceName]
        weightWorkspace = self.mantidSnapper.mtd[self.weightWorkspaceName]

        numSpec = weightWorkspace.getNumberHistograms()

        allOnes = all(np.all(weightWorkspace.readY(index) == 1) for index in range(numSpec))

        if allOnes:
            self.mantidSnapper.CloneWorkspace(
                "Cloning inputworkspace...",
                InputWorkspace=self.weightWorkspaceName,
                OutputWorkspace=self.outputWorkspaceName,
            )
            self.mantidSnapper.executeQueue()
            outputWorkspace = self.mantidSnapper.mtd[self.outputWorkspaceName]
            for index in range(numSpec):
                zeroDataX = np.zeros_like(weightWorkspace.readX(index))
                outputWorkspace.dataX(index)[:] = zeroDataX
                zeroDataY = np.zeros_like(weightWorkspace.readY(index))
                outputWorkspace.dataY(index)[:] = zeroDataY
            self.mantidSnapper.WashDishes(
                "Cleaning up weight workspace...",
                Workspace=self.weightWorkspaceName,
            )
            self.mantidSnapper.executeQueue()
            self.setProperty("OutputWorkspace", outputWorkspace)
            return
        else:
            # extract x & y data for csaps
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


# Register algorithm with Mantid
AlgorithmFactory.subscribe(SmoothDataExcludingPeaksAlgo)
