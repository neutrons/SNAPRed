import json
from typing import Dict

import numpy as np
from mantid.api import AlgorithmFactory, IEventWorkspace, MatrixWorkspaceProperty, PropertyMode, PythonAlgorithm
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

    def unbagGroceries(self):
        self.inputWorkspaceName = self.getPropertyValue("InputWorkspace")
        self.weightWorkspaceName = self.getPropertyValue("WeightWorkspace")

    def validateInputs(self) -> Dict[str, str]:
        errors = {}

        if self.getProperty("WeightWorkspace").isDefault:
            errors["WeightWorkspace"] = "Weight calculator requires name for weight workspace"

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
            OutputWorkspace=self.weightWorkspaceName,
        )
        self.mantidSnapper.executeQueue()
        weight_ws = self.mantidSnapper.mtd[self.weightWorkspaceName]
        isEventWorkspace: bool = isinstance(weight_ws, IEventWorkspace)
        if isEventWorkspace:
            self.mantidSnapper.ConvertToMatrixWorkspace(
                "Converting event workspace to histogram workspace",
                InputWorkspace=self.weightWorkspaceName,
                OutputWorkspace=self.weightWorkspaceName,
            )
            # self.mantidSnapper.RebinToWorkspace(
            #     "Rebin to remvoe events",
            #     WorkspaceToRebin = self.weightWorkspaceName,
            #     WorkspaceToMatch = self.weightWorkspaceName,
            #     OutputWorkspace = self.weightWorkspaceName,
            #     PreserveEvents=False,
            # )
            self.mantidSnapper.executeQueue()

        weight_ws = self.mantidSnapper.mtd[self.weightWorkspaceName]
        for index, groupID in enumerate(self.groupIDs):
            # get spectrum X,Y
            x = weight_ws.readX(index)
            y = weight_ws.readY(index)
            # create and initialize a weights array
            weights = np.ones(len(y))
            # for each peak extent, set zeros to the weights array
            for peak in self.predictedPeaks[groupID]:
                mask_indices = np.where(np.logical_and(x > peak.position.minimum, x < peak.position.maximum))[0]
                mask_indices = mask_indices[mask_indices < len(weights)]
                weights[mask_indices] = 0.0
            weight_ws.setY(index, weights)

        self.setProperty("WeightWorkspace", weight_ws)

        if isEventWorkspace:
            self.mantidSnapper.ConvertToEventWorkspace(
                "Converting histogram workspace back to event workspace",
                InputWorkspace=self.weightWorkspaceName,
                OutputWorkspace=self.weightWorkspaceName,
            )

        self.mantidSnapper.executeQueue()


AlgorithmFactory.subscribe(DiffractionSpectrumWeightCalculator)
