import json
import numpy as np
import pdb;

from csaps import csaps
from mantid.api import *
from mantid.api import (
    AlgorithmFactory,
    PythonAlgorithm,
    WorkspaceGroup,
)
from mantid.kernel import Direction

from snapred.backend.dao.CrystallographicInfo import CrystallographicInfo
from snapred.backend.dao.state.InstrumentState import InstrumentState
from snapred.backend.recipe.algorithm.DetectorPeakPredictor import DetectorPeakPredictor
from snapred.backend.recipe.algorithm.DiffractionSpectrumWeightCalculator import DiffractionSpectrumWeightCalculator
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper

name = "SmoothDataExcludingPeaks"


class SmoothDataExcludingPeaks(PythonAlgorithm):
    def PyInit(self):
        # declare properties
        self.declareProperty("InputWorkspace", defaultValue="", direction=Direction.Input)
        self.declareProperty("InstrumentState", defaultValue="", direction=Direction.Input)
        self.declareProperty("CrystalInfo", defaultValue="", direction=Direction.Input)
        self.declareProperty("OutputWorkspace", defaultValue="", direction=Direction.Output)
        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, name)

    def PyExec(self):
        self.log().notice("Removing peaks and smoothing data")

        # load workspace
        input_ws = self.getProperty("InputWorkspace").value
        ws = self.mantidSnapper.mtd[input_ws]

        # load instrument state
        instrumentState = self.getProperty("InstrumentState").value

        # load crystal info
        crystalInfo = self.getProperty("CrystalInfo").value

        # call the diffraction spectrum weight calculator
        weight_ws_name = "weight_ws"
        weightCalAlgo = DiffractionSpectrumWeightCalculator()
        weightCalAlgo.initialize()
        weightCalAlgo.setProperty("InputWorkspace", input_ws)
        weightCalAlgo.setProperty("InstrumentState", instrumentState)
        weightCalAlgo.setProperty("CrystalInfo", crystalInfo)
        weightCalAlgo.setProperty("WeightWorkspace", weight_ws_name)
        weightCalAlgo.execute()
<<<<<<< HEAD
        weightCalAlgo.getProperty("WeightWorkspace").value
=======
        weights_ws = mtd[weight_ws_name]

        # create workspace group
        ws_group = WorkspaceGroup()
        mtd.add("SmoothedDataExcludingPeaks", ws_group)

        numSpec = weights_ws.getNumberHistograms()
>>>>>>> 26455a0 (Csaps errors!! outdata_{index value}.txt created to include printed values of x_midpoints array as per Michael's request for further testing.)

        # extract x & y data for csaps
        for index in range(numSpec):
            x = weights_ws.readX(index) # len of 1794
            w = weights_ws.readY(index) # len of 1793
            y = ws.readY(index)
            x_midpoints = (x[:-1] + x[1:]) / 2 # len of 1793

            with open(f'./outdata_{index}.txt', 'w') as file: file.write(",".join(map(str, x_midpoints)))

            # pdb.set_trace()
            # smoothing_results = csaps(x_midpoints, y)
            # yi = smoothing_result.values
            # single_spectrum_ws_name = f"{input_ws}_temp_single_spectrum_{index}"
            # self.mantidSnapper.CreateWorkspace(
            #     DataX=x, DataY=yi, NSpec=index + 1, OutputWorkspace=single_spectrum_ws_name
            # )
            # ws_group.add(single_spectrum_ws_name)
            # # execute mantidsnapper
            # self.mantidSnapper.executeQueue()
            # self.setProperty("OutputWorkspace", ws_group.name())
            # return ws_group

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

# Register algorithm with Mantid
AlgorithmFactory.subscribe(SmoothDataExcludingPeaks)