import json

import numpy as np
from mantid.api import AlgorithmFactory, PythonAlgorithm
from mantid.kernel import Direction

from snapred.backend.dao.DetectorPeak import DetectorPeak
from snapred.backend.dao.GroupPeakList import GroupPeakList
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

    def chopIngredients(self):
        # get the peak predictions from user input, or load them
        if self.getProperty("DetectorPeaks").isDefault:
            peakString = self.mantidSnapper.DetectorPeakPredictor(
                "Predicting peaks...",
                InstrumentState=self.getProperty("InstrumentState").value,
                CrystalInfo=self.getProperty("CrystalInfo").value,
                PeakIntensityFractionThreshold=0.0,
            )
            self.mantidSnapper.executeQueue()
        else:
            peakString = self.getPropertyValue("DetectorPeaks")
        predictedPeaksList = json.loads(str(peakString))

        self.groupIDs = []
        self.predictedPeaks = {}
        for prediction in predictedPeaksList:
            groupPeakList = GroupPeakList.parse_obj(prediction)
            self.groupIDs.append(groupPeakList.groupID)
            self.predictedPeaks[groupPeakList.groupID] = groupPeakList.peaks

    def unbagGroceries(self):
        self.inputWorkspaceName = self.getPropertyValue("InputWorkspace")
        if self.getProperty("WeightWorkspace").isDefault:
            self.outputWorkspaceName = "tmp_weight_ws"
            self.setPropertyValue("WeightWorkspace", self.outputWorkspaceName)
        else:
            self.outputWorkspaceName = self.getPropertyValue("WeightWorkspace")

    def PyExec(self):
        self.chopIngredients()
        self.unbagGroceries()

        # clone input workspace to create a weight workspace
        self.mantidSnapper.CloneWorkspace(
            "Cloning a weighting workspce...",
            InputWorkspace=self.inputWorkspaceName,
            OutputWorkspace=self.outputWorkspaceName,
        )
        self.mantidSnapper.executeQueue()
        weight_ws = self.mantidSnapper.mtd[self.outputWorkspaceName]

        for index, groupID in enumerate(self.groupIDs):
            # get spectrum X,Y
            x = weight_ws.readX(index)
            y = weight_ws.readY(index)
            # create and initialize a weights array
            weights = np.ones(len(y))
            # for each peak extent, set zeros to the weights array
            for peak in self.predictedPeaks[groupID]:
                mask_indices = np.where(np.logical_and(x > peak.position.minimum, x < peak.position.maximum))
                weights[mask_indices] = 0.0
            weight_ws.setY(index, weights)

        return weight_ws


AlgorithmFactory.subscribe(DiffractionSpectrumWeightCalculator)
