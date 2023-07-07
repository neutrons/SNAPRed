import json

from mantid.api import (
    AlgorithmFactory, 
    PythonAlgorithm,
    mtd,
)
from mantid.kernel import Direction
from mantid.api import *
from csaps import csaps

from snapred.backend.recipe.algorithm.IngestCrystallographicInfoAlgorithm import IngestCrystallographicInfoAlgorithm
from snapred.backend.dao.calibration.Calibration import Calibration
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper


name = "SmoothDataExcludingPeaks"


class SmoothDataExcludingPeaks(PythonAlgorithm):
    def PyInit(self):
        # declare properties
        self.declareProperty("InputWorkspace", defaultValue = "", direction = Direction.Input)
        self.declareProperty("nxsPath", defaultValue = "", direction = Direction.Input)
        self.declareProperty("OutputWorkspace", defaultValue = "", direction = Direction.Output)
        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, name)
    
    def PyExec(self):
        self.log().notice("Removing peaks and smoothing data")

        # load nexus file & load workspace
        nxsPath = self.getProperty("nxsPath").value
        ws = self.mantidSnapper.LoadNexus(Filename = nxsPath, SpectrumMin = 1, EntryNumber = 1)
        ws = self.mantidSnapper.mtd[ws]
        numSpec = ws.getNumberHistograms()

        # load crystal info
        crystalInfo = IngestCrystallographicInfoAlgorithm.parse_raw(self.getProperty("crystalInfo"))

        # load calibration
        calibrationState = Calibration.parse_raw(self.getProperty("InputState").value)
        instrumentState = calibrationState.instrumentState

        for index in range(numSpec):
            temp_ws_name = f"{ws}_temp_workspace_{index}"
            temp_ws = self.mantidSnapper.CreateWorkspace(temp_ws_name)
            
            # call the diffraction spectrum peak predictor
            peaks = []
            predictorAlgo = 

        # load nexus
        # num spe = num groups (instrument state) --> consitant with workspace
        # state & crystal info --> consitant with workspace
        # call diffraction spectrum peak predictor --> to get peaks (num groups)
        # call diffraction spectrum weight calc (send one spectra only)
        # extract x and y data from a workspace (numpy arrray)
        # implement csaps        
        # create new workspace with csaps data

# Register algorithm with Mantid
AlgorithmFactory.subscribe(SmoothDataExcludingPeaks)