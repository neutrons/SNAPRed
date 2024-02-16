import json
from typing import Dict, List

import numpy as np
from mantid.api import AlgorithmFactory, IEventWorkspace, MatrixWorkspaceProperty, PropertyMode, PythonAlgorithm
from mantid.kernel import Direction
from pydantic import parse_raw_as

from snapred.backend.dao.GroupPeakList import GroupPeakList
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.algorithm.DetectorPeakPredictor import DetectorPeakPredictor
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
        self.declareProperty("DetectorPeakIngredients", defaultValue="", direction=Direction.Input)
        self.declareProperty("DetectorPeaks", defaultValue="", direction=Direction.Input)

        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, __name__)

    def chopIngredients(self):
        pass

    def unbagGroceries(self):
        self.inputWorkspaceName = self.getPropertyValue("InputWorkspace")
        self.weightWorkspaceName = self.getPropertyValue("WeightWorkspace")

    def validateInputs(self) -> Dict[str, str]:
        errors = {}
        waysToGetPeaks = ["DetectorPeaks", "DetectorPeakIngredients"]
        definedWaysToGetPeaks = [x for x in waysToGetPeaks if not self.getProperty(x).isDefault]
        if len(definedWaysToGetPeaks) == 0:
            msg = """
            Must specify either DetectorPeaks,
            OR DetectorPeakIngredients to generate the peaks
            """
            errors["DetectorPeaks"] = msg
            errors["DetectorPeakIngredients"] = msg
        elif len(definedWaysToGetPeaks) == 2:
            logger.warn(
                """Both a list of detector peaks and ingredients were given;
                the list will be used and ingredients ignored"""
            )
        return errors

    def PyExec(self):
        self.chopIngredients()
        self.unbagGroceries()

        # get the peak predictions from user input, or load them
        if not self.getProperty("DetectorPeaks").isDefault:
            peakString = self.getPropertyValue("DetectorPeaks")
        else:
            peakString = self.mantidSnapper.DetectorPeakPredictor(
                "Predicting peaks...",
                Ingredients=self.getPropertyValue("DetectorPeakIngredients"),
            )
            self.mantidSnapper.executeQueue()

        predictedPeaksList = parse_raw_as(List[GroupPeakList], str(peakString))

        self.groupIDs = []
        self.predictedPeaks = {}
        for prediction in predictedPeaksList:
            groupPeakList = GroupPeakList.parse_obj(prediction)
            self.groupIDs.append(groupPeakList.groupID)
            self.predictedPeaks[groupPeakList.groupID] = groupPeakList.peaks

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

        if isEventWorkspace:
            self.mantidSnapper.ConvertToEventWorkspace(
                "Converting histogram workspace back to event workspace",
                InputWorkspace=self.weightWorkspaceName,
                OutputWorkspace=self.weightWorkspaceName,
            )
        self.mantidSnapper.executeQueue()
        self.setPropertyValue("WeightWorkspace", self.weightWorkspaceName)


AlgorithmFactory.subscribe(DiffractionSpectrumWeightCalculator)
