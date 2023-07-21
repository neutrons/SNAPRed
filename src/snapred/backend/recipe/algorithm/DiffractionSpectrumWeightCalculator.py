import json

import numpy as np
from mantid.api import AlgorithmFactory, PythonAlgorithm
from mantid.kernel import Direction

from snapred.backend.dao.DetectorPeak import DetectorPeak
from snapred.backend.recipe.algorithm.DetectorPeakPredictor import DetectorPeakPredictor
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper

name = "DiffractionSpectrumWeightCalculator"


class DiffractionSpectrumWeightCalculator(PythonAlgorithm):
    def PyInit(self):
        # declare properties
        self.declareProperty("InputWorkspace", defaultValue="", direction=Direction.Input)
        self.declareProperty("InstrumentState", defaultValue="", direction=Direction.Input)
        self.declareProperty("CrystalInfo", defaultValue="", direction=Direction.Input)
        self.declareProperty("DetectorPeaks", defaultValue="", direction=Direction.Input)
        self.declareProperty(
            "WeightWorkspace", defaultValue="", direction=Direction.Input
        )  # name of the output workspace to be created by the algorithm

        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, name)

    def PyExec(self):
        # get or generate predicted detector peaks
        predictedPeaksInput = self.getProperty("DetectorPeaks").value
        if predictedPeaksInput != "":
            predictedPeaks_json = json.loads(predictedPeaksInput)
        else:
            result = self.mantidSnapper.DetectorPeakPredictor(
                "Predicting peaks...",
                InstrumentState=self.getProperty("InstrumentState").value,
                CrystalInfo=self.getProperty("CrystalInfo").value,
            )
            self.mantidSnapper.executeQueue()
            predictedPeaks_json = json.loads(result.get())

        # clone input workspace to create a weight workspace
        input_ws_name = self.getProperty("InputWorkspace").value
        weight_ws_name = self.getProperty("WeightWorkspace").value
        self.mantidSnapper.CloneWorkspace(
            f"Cloning {input_ws_name} to {weight_ws_name}...",
            InputWorkspace=input_ws_name,
            OutputWorkspace=weight_ws_name,
        )
        self.mantidSnapper.executeQueue()
        weight_ws = self.mantidSnapper.mtd[weight_ws_name]

        numSpec = weight_ws.getNumberHistograms()
        for index in range(numSpec):
            # get spectrum X,Y
            x = weight_ws.readX(index)
            y = weight_ws.readY(index)
            # create and initialize a weights array
            weights = np.ones(len(y))
            # get peaks predicted for this spectrum
            spectrumPredictedPeaks_json = predictedPeaks_json[index]
            # for each peak extent, set zeros to the weights array
            for peak_json in spectrumPredictedPeaks_json:
                peak = DetectorPeak.parse_raw(peak_json)
                mask_indices = np.where(np.logical_and(x > peak.position.minimum, x < peak.position.maximum))
                weights[mask_indices] = 0.0
            weight_ws.setY(index, weights)

        return weight_ws


AlgorithmFactory.subscribe(DiffractionSpectrumWeightCalculator)
