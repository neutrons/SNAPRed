import json
from typing import Dict, List

import numpy as np
from mantid.api import AlgorithmFactory, IEventWorkspace, MatrixWorkspaceProperty, PropertyMode, PythonAlgorithm
from mantid.kernel import Direction
from pydantic import parse_raw_as

from snapred.backend.dao.GroupPeakList import GroupPeakList
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper

logger = snapredLogger.getLogger(__name__)


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
            MatrixWorkspaceProperty("WeightWorkspace", "", Direction.Output, PropertyMode.Mandatory),
            doc="The output workspace to be created by the algorithm",
        )
        self.declareProperty("DetectorPeaks", defaultValue="", direction=Direction.Input)

        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, __name__)

    def chopIngredients(self, ingredients: List[GroupPeakList]):
        self.groupIDs = []
        self.predictedPeaks = {}
        for prediction in ingredients:
            groupPeakList = GroupPeakList.parse_obj(prediction)
            self.groupIDs.append(groupPeakList.groupID)
            self.predictedPeaks[groupPeakList.groupID] = groupPeakList.peaks

    def unbagGroceries(self):
        self.inputWorkspaceName = self.getPropertyValue("InputWorkspace")
        self.weightWorkspaceName = self.getPropertyValue("WeightWorkspace")
        # clone input workspace to create a weight workspace
        self.mantidSnapper.CloneWorkspace(
            "Cloning a weighting workspce...",
            InputWorkspace=self.inputWorkspaceName,
            OutputWorkspace=self.weightWorkspaceName,
        )
        # check if this is event data, and if so covnert it to historgram data
        self.isEventWorkspace = isinstance(self.mantidSnapper.mtd[self.inputWorkspaceName], IEventWorkspace)
        if self.isEventWorkspace:
            self.mantidSnapper.ConvertToMatrixWorkspace(
                "Converting event workspace to histogram workspace",
                InputWorkspace=self.weightWorkspaceName,
                OutputWorkspace=self.weightWorkspaceName,
            )
        self.mantidSnapper.executeQueue()

    def validateInputs(self) -> Dict[str, str]:
        errors = {}
        return errors

    def PyExec(self):
        predictedPeaksList = parse_raw_as(List[GroupPeakList], self.getPropertyValue("DetectorPeaks"))
        self.chopIngredients(predictedPeaksList)
        self.unbagGroceries()

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

        if self.isEventWorkspace:
            self.mantidSnapper.ConvertToEventWorkspace(
                "Converting histogram workspace back to event workspace",
                InputWorkspace=self.weightWorkspaceName,
                OutputWorkspace=self.weightWorkspaceName,
            )
        self.mantidSnapper.executeQueue()
        self.setPropertyValue("WeightWorkspace", self.weightWorkspaceName)


AlgorithmFactory.subscribe(DiffractionSpectrumWeightCalculator)
