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

from mantid.api import (
    AlgorithmFactory,
    PythonAlgorithm,
    WorkspaceGroup,
    mtd,
)
from mantid.kernel import Direction
from scipy.interpolate import splev, splrep

from snapred.backend.dao.SmoothDataExcludingPeaksIngredients import SmoothDataExcludingPeaksIngredients
from snapred.backend.recipe.algorithm.DiffractionSpectrumWeightCalculator import DiffractionSpectrumWeightCalculator
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper

name = "SmoothDataExcludingPeaks"


# TODO: Rename so it matches filename
class SmoothDataExcludingPeaks(PythonAlgorithm):
    def PyInit(self):
        # declare properties
        self.declareProperty("InputWorkspace", defaultValue="", direction=Direction.Input)
        self.declareProperty("SmoothDataExcludingPeaksIngredients", defaultValue="", direction=Direction.Input)
        self.declareProperty("OutputWorkspace", defaultValue="SmoothedDataExcludingPeaks", direction=Direction.Output)
        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, name)

    def PyExec(self):
        self.log().notice("Removing peaks and smoothing data")

        outputWorkspaceName = self.getProperty("OutputWorkspace").value

        # load ingredients
        smoothdataIngredients = SmoothDataExcludingPeaksIngredients(
            **json.loads(self.getProperty("SmoothDataExcludingPeaksIngredients").value)
        )

        # load workspace
        input_ws = self.getProperty("InputWorkspace").value

        # load instrument state
        instrumentState = smoothdataIngredients.instrumentState

        # load crystal info
        crystalInfo = smoothdataIngredients.crystalInfo

        ws = self.mantidSnapper.mtd[input_ws]

        # call the diffraction spectrum weight calculator
        weight_ws_name = "weight_ws"
        self.mantidSnapper.DiffractionSpectrumWeightCalculator(
            "Calculating spectrum weights...",
            InputWorkspace=input_ws,
            InstrumentState=instrumentState.json(),
            CrystalInfo=crystalInfo.json(),
            WeightWorkspace=weight_ws_name,
        )
        self.mantidSnapper.executeQueue()
        weights_ws = self.mantidSnapper.mtd[weight_ws_name]

        # get number of spectrum to iterate over
        numSpec = weights_ws.getNumberHistograms()

        # extract x & y data for csaps
        dataX = []
        dataY = []
        for index in range(numSpec):
            x = weights_ws.readX(index)  # len of 1794
            w = weights_ws.readY(index)  # len of 1793
            y = ws.readY(index)
            x_midpoints = (x[:-1] + x[1:]) / 2  # len of 1793
            x_list = x_midpoints.tolist()
            y_list = y.tolist()
            w_list = w.tolist()
            joined_list = []
            joined_list = []

            # joining weight array with y values array
            for i, y_value in enumerate(y_list):
                joined_list.append(y_value * w_list[i])
            tck = splrep(x_list, joined_list)
            smoothing_results = splev(x_list, tck)
            dataX.extend(x)
            dataY.extend(smoothing_results)

        self.mantidSnapper.CreateWorkspace(
            "Creating new workspace for smoothed spectrum data...",
            DataX=dataX,
            DataY=dataY,
            NSpec=numSpec,
            OutputWorkspace=outputWorkspaceName,
        )
        self.mantidSnapper.DeleteWorkspace("Cleaning up weight workspace...", Workspace=weight_ws_name)
        # execute mantidsnapper
        self.mantidSnapper.executeQueue()

        self.setProperty("OutputWorkspace", outputWorkspaceName)
        return outputWorkspaceName


# Register algorithm with Mantid
AlgorithmFactory.subscribe(SmoothDataExcludingPeaks)
