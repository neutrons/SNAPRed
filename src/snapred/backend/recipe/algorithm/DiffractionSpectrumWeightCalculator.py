from typing import Dict, List

import numpy as np
from mantid.api import IEventWorkspace, MatrixWorkspaceProperty, PropertyMode, PythonAlgorithm
from mantid.kernel import Direction
from mantid.kernel import ULongLongPropertyWithValue as PointerProperty
from mantid.simpleapi import (
    CloneWorkspace,
    ConvertToEventWorkspace,
    ConvertToMatrixWorkspace,
    mtd,
)

from snapred.backend.dao.GroupPeakList import GroupPeakList
from snapred.backend.log.logger import snapredLogger
from snapred.meta.pointer import access_pointer, inspect_pointer

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
        self.declareProperty(
            PointerProperty("DetectorPeaks", id(None)),
            doc="The memory address pointing to the list of grouped peaks.",
        )

        self.setRethrows(True)

    def chopIngredients(self, ingredients: List[GroupPeakList]):
        self.groupIDs = []
        self.predictedPeaks = {}
        for prediction in ingredients:
            groupPeakList = GroupPeakList.model_validate(prediction)
            self.groupIDs.append(groupPeakList.groupID)
            self.predictedPeaks[groupPeakList.groupID] = groupPeakList.peaks

    def unbagGroceries(self):
        self.inputWorkspaceName = self.getPropertyValue("InputWorkspace")
        self.weightWorkspaceName = self.getPropertyValue("WeightWorkspace")
        # clone input workspace to create a weight workspace
        CloneWorkspace(
            InputWorkspace=self.inputWorkspaceName,
            OutputWorkspace=self.weightWorkspaceName,
        )
        # check if this is event data, and if so covnert it to historgram data
        self.isEventWorkspace = isinstance(mtd[self.inputWorkspaceName], IEventWorkspace)
        if self.isEventWorkspace:
            ConvertToMatrixWorkspace(
                InputWorkspace=self.weightWorkspaceName,
                OutputWorkspace=self.weightWorkspaceName,
            )

    def validateInputs(self) -> Dict[str, str]:
        errors = {}
        ws = self.getProperty("InputWorkspace").value
        ingredients = inspect_pointer(self.getProperty("DetectorPeaks").value)
        if ws.getNumberHistograms() != len(ingredients):
            msg = f"""
            Number of histograms {ws.getNumberHistograms()}
            incompatible with groups in DetectorPeaks {len(ingredients)}
            """
            errors["InputWorkspace"] = msg
            errors["DetectorPeaks"] = msg
        return errors

    def PyExec(self):
        peak_ptr: PointerProperty = self.getProperty("DetectorPeaks").value
        predictedPeaksList = access_pointer(peak_ptr)
        self.chopIngredients(predictedPeaksList)
        self.unbagGroceries()

        weight_ws = mtd[self.weightWorkspaceName]
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
            ConvertToEventWorkspace(
                InputWorkspace=self.weightWorkspaceName,
                OutputWorkspace=self.weightWorkspaceName,
            )
        self.setPropertyValue("WeightWorkspace", self.weightWorkspaceName)
