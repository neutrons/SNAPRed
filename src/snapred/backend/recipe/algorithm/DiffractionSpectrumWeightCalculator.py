import json
from typing import Dict

import numpy as np
from mantid.api import (
    AlgorithmFactory,
    MatrixWorkspaceProperty,
    PropertyMode,
    PythonAlgorithm,
)
from mantid.kernel import Direction

from snapred.backend.dao.DetectorPeak import DetectorPeak
from snapred.backend.dao.GroupPeakList import GroupPeakList
from snapred.backend.recipe.algorithm.DetectorPeakPredictor import DetectorPeakPredictor
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper


class DiffractionSpectrumWeightCalculator(PythonAlgorithm):
    def category(self):
        return "SNAPRed Internal"

    def PyInit(self):
        # declare properties
        self.declareProperty(
            MatrixWorkspaceProperty("InputWorkspace", "", Direction.Input, PropertyMode.Mandatory),
            doc="Workspace peaks to be weighted",
        )
        self.declareProperty(
            MatrixWorkspaceProperty("WeightWorkspace", "", Direction.Output, PropertyMode.Optional),
            doc="The output workspace to be created by the algorithm",
        )
        self.declareProperty("InstrumentState", defaultValue="", direction=Direction.Input)
        self.declareProperty("CrystalInfo", defaultValue="", direction=Direction.Input)
        self.declareProperty("DetectorPeaks", defaultValue="", direction=Direction.Input)

        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, __name__)

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
        print(self.groupIDs)
        print(self.predictedPeaks)

    def unbagGroceries(self):
        self.inputWorkspaceName = self.getPropertyValue("InputWorkspace")
        self.outputWorkspaceName = self.getPropertyValue("WeightWorkspace")

    def validateInputs(self) -> Dict[str, str]:
        errors = {}

        if self.getProperty("WeightWorkspace").isDefault:
            errors["WeightWorkspace"] = "Weghit calculator requires namefor weight worspace"

        if self.getProperty("DetectorPeaks").isDefault:
            noState = self.getProperty("InstrumentState").isDefault
            noXtal = self.getProperty("CrystalInfo").isDefault
            if noState or noXtal:
                msg = """
                must specify either detector peaks,
                OR both crystal info and instrument state
                """
                errors["DetectorPeaks"] = msg
                errors["InstrumentState"] = msg
                errors["CrystalInfo"] = msg
        return errors

    def PyExec(self):
        self.chopIngredients()
        self.unbagGroceries()

        # clone input workspace to create a weight workspace
        self.mantidSnapper.CloneWorkspace(
            "Cloning a weighting workspce...",
            InputWorkspace=self.inputWorkspaceName,
            OutputWorkspace=self.outputWorkspaceName,
        )
        print(self.groupIDs)
        print(self.predictedPeaks)
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
