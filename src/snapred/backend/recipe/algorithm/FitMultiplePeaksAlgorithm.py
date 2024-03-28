import json
from enum import Enum
from typing import Dict, List

import numpy as np
from mantid.api import (
    AlgorithmFactory,
    MatrixWorkspaceProperty,
    PropertyMode,
    PythonAlgorithm,
    WorkspaceFactory,
    WorkspaceGroup,
    mtd,
)
from mantid.kernel import Direction, StringListValidator
from mantid.simpleapi import DeleteWorkspaces
from pydantic import parse_raw_as

from snapred.backend.dao.GroupPeakList import GroupPeakList
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper
from snapred.backend.recipe.algorithm.PurgeOverlappingPeaksAlgorithm import PurgeOverlappingPeaksAlgorithm
from snapred.meta.Config import Config
from snapred.meta.mantid.AllowedPeakTypes import allowed_peak_type_list

logger = snapredLogger.getLogger(__name__)


class FitOutputEnum(Enum):
    PeakPosition = 0
    Parameters = 1
    Workspace = 2
    ParameterError = 3


class FitMultiplePeaksAlgorithm(PythonAlgorithm):
    NOISE_2_MIN = Config["calibration.fitting.minSignal2Noise"]

    def category(self):
        return "SNAPRed Data Processing"

    def PyInit(self):
        # declare properties
        self.declareProperty(
            MatrixWorkspaceProperty("InputWorkspace", "", Direction.Input, PropertyMode.Mandatory),
            doc="Workspace containing the peaks to be fit",
        )
        self.declareProperty(
            "DetectorPeaks",
            defaultValue="",
            direction=Direction.Input,
            doc="Input list of peaks to be fit",
        )
        self.declareProperty(
            "PeakFunction", "Gaussian", StringListValidator(allowed_peak_type_list), direction=Direction.Input
        )
        self.declareProperty("OutputWorkspaceGroup", defaultValue="fitPeaksWSGroup", direction=Direction.Output)
        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, __name__)

    def listToWorkspace(self, aList, name):
        ws = WorkspaceFactory.create("Workspace2D", NVectors=1, XLength=len(aList), YLength=len(aList))
        ws.setX(0, np.asarray(aList))
        # register ws in mtd
        mtd.addOrReplace(name, ws)
        return ws

    def validateInputs(self) -> Dict[str, str]:
        errors = {}
        return errors

    def chopIngredients(self, ingredients: List[GroupPeakList]):
        self.groupIDs = []
        self.reducedList = {}
        for groupPeakList in ingredients:
            self.groupIDs.append(groupPeakList.groupID)
            self.reducedList[groupPeakList.groupID] = groupPeakList.peaks

    def unbagGroceries(self):
        self.inputWorkspaceName = self.getPropertyValue("Inputworkspace")
        self.outputWorkspaceName = self.getPropertyValue("OutputWorkspaceGroup")
        self.outputWorkspace = WorkspaceGroup()
        if mtd.doesExist(self.outputWorkspaceName):
            DeleteWorkspaces(list(mtd[self.outputWorkspaceName].getNames()))
        mtd.addOrReplace(self.outputWorkspaceName, self.outputWorkspace)

    def PyExec(self):
        peakFunction = self.getPropertyValue("PeakFunction")
        reducedPeakList = parse_raw_as(List[GroupPeakList], self.getPropertyValue("DetectorPeaks"))
        self.chopIngredients(reducedPeakList)
        self.unbagGroceries()

        for index, groupID in enumerate(self.groupIDs):
            outputNames = [None] * len(FitOutputEnum)
            outputNames[FitOutputEnum.PeakPosition.value] = f"{self.outputWorkspaceName}_fitted_peakpositions_{index}"
            outputNames[FitOutputEnum.Parameters.value] = f"{self.outputWorkspaceName}_fitted_params_{index}"
            outputNames[FitOutputEnum.Workspace.value] = f"{self.outputWorkspaceName}_fitted_{index}"
            outputNames[FitOutputEnum.ParameterError.value] = f"{self.outputWorkspaceName}_fitted_params_err_{index}"

            peakCenters = []
            peakLimits = []
            for peak in self.reducedList[groupID]:
                peakCenters.append(peak.position.value)
                peakLimits.extend([peak.position.minimum, peak.position.maximum])

            self.mantidSnapper.ExtractSingleSpectrum(
                "Extract Single Spectrum...",
                InputWorkspace=self.inputWorkspaceName,
                OutputWorkspace="ws2fit",
                WorkspaceIndex=index,
            )

            self.mantidSnapper.FitPeaks(
                "Fit Peaks...",
                InputWorkspace="ws2fit",
                PeakCenters=",".join(np.array(peakCenters).astype("str")),
                PeakFunction=peakFunction,
                FitWindowBoundaryList=",".join(np.array(peakLimits).astype("str")),
                OutputWorkspace=outputNames[FitOutputEnum.PeakPosition.value],
                OutputPeakParametersWorkspace=outputNames[FitOutputEnum.Parameters.value],
                BackgroundType="Quadratic",
                FittedPeaksWorkspace=outputNames[FitOutputEnum.Workspace.value],
                ConstrainPeakPositions=True,
                OutputParameterFitErrorsWorkspace=outputNames[FitOutputEnum.ParameterError.value],
                MinimumSignalToNoiseRatio=self.NOISE_2_MIN,
            )
            self.mantidSnapper.executeQueue()
            for output in outputNames:
                self.outputWorkspace.add(output)

        self.mantidSnapper.WashDishes(
            "Deleting fitting workspace...",
            Workspace="ws2fit",
        )
        self.mantidSnapper.executeQueue()
        self.setProperty("OutputWorkspaceGroup", self.outputWorkspace.name())


AlgorithmFactory.subscribe(FitMultiplePeaksAlgorithm)
