import json

from csaps import csaps
from mantid.api import *
from mantid.api import (
    AlgorithmFactory,
    PythonAlgorithm,
)
from mantid.kernel import Direction

from snapred.backend.dao.calibration.Calibration import Calibration
from snapred.backend.recipe.algorithm.DetectorPeakPredictor import DetectorPeakPredictor
from snapred.backend.recipe.algorithm.DiffractionSpectrumWeightCalculator import DiffractionSpectrumWeightCalculator
from snapred.backend.recipe.algorithm.IngestCrystallographicInfoAlgorithm import IngestCrystallographicInfoAlgorithm
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper

name = "SmoothDataExcludingPeaks"


class SmoothDataExcludingPeaks(PythonAlgorithm):
    def PyInit(self):
        # declare properties
        self.declareProperty("InputWorkspace", defaultValue="", direction=Direction.Input)
        self.declareProperty("OutputWorkspace", defaultValue="", direction=Direction.Output)
        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, name)

    def PyExec(self):
        self.log().notice("Removing peaks and smoothing data")

        # load workspace
        input_ws = self.getProperty("InputWorkspace").value
        ws = self.mantidSnapper.mtd[input_ws]
        numSpec = ws.getNumberHistograms()

        # load crystal info
        crystalInfo = IngestCrystallographicInfoAlgorithm.parse_raw(self.getProperty("crystalInfo"))

        # load calibration
        calibrationState = Calibration.parse_raw(self.getProperty("InputState").value)
        instrumentState = calibrationState.instrumentState

        # call the diffraction spectrum peak predictor
        peaks = []
        # TODO: Need to replace theses methods with Robert's script, just pass for now.
        predictorAlgo = DetectorPeakPredictor()
        predictorAlgo.initialize()
        predictorAlgo.setProperty("InstrumentState", instrumentState.json())
        predictorAlgo.setProperty("CrystalInfo", crystalInfo.json())
        predictorAlgo.setProperty("NumFocusGroups", numSpec)
        predictorAlgo.execute()
        peaks = json.loads(predictorAlgo.getProperty("DetectorPeaks").value)

        # call the diffraction spectrum weight calculator
        weightCalAlgo = DiffractionSpectrumWeightCalculator()
        weightCalAlgo.initialize()
        weightCalAlgo.setProperty("InputWorkspace", ws)
        weightCalAlgo.setProperty("PredictedPeaks", peaks)  # only pass single peak at a time
        weightCalAlgo.execute()
        json.loads(weightCalAlgo.getProperty("WeightWorkspace").value)

        # extract x & y data for csaps
        x = []
        y = []
        for index in len(numSpec):
            x = ws.readX(index)
            y = ws.readY(index)
            smoothing_result = csaps(x, y, xi = len(x))
            yi = smoothing_result.values
            smooth = smoothing_result.smooth
            single_spectrum_ws_name = f"{ws}_temp_single_spectrum_{index}"
            self.mantidSnapper.CreateWorkspace(DataX = x, DataY = yi, NSpec = index + 1, 
            OutputWorkspace = single_spectrum_ws_name)
        
            # execute mantidsnapper
            self.mantidSnapper.executeQueue()


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
