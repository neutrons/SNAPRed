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
from snapred.backend.recipe.algorithm.ConjoinTableWorkspaces import ConjoinTableWorkspaces
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper
from snapred.meta.Config import Config
from snapred.meta.mantid.AllowedPeakTypes import allowed_peak_type_list

logger = snapredLogger.getLogger(__name__)


class FitOutputEnum(Enum):
    PeakPosition = 0
    Parameters = 1
    Workspace = 2
    ParameterError = 3


class FitMultiplePeaksAlgorithm(PythonAlgorithm):
    NOYZE_2_MIN = Config["calibration.fitting.minSignal2Noise"]

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
            "PeakType", "Gaussian", StringListValidator(allowed_peak_type_list), direction=Direction.Input
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
        # suffixes to name diagnostic output
        self.outputSuffix = [None] * len(FitOutputEnum)
        self.outputSuffix[FitOutputEnum.PeakPosition.value] = "_dspacing"
        self.outputSuffix[FitOutputEnum.Workspace.value] = "_fitted"
        self.outputSuffix[FitOutputEnum.Parameters.value] = "_fitparam"
        self.outputSuffix[FitOutputEnum.ParameterError.value] = "_fiterror"

    def unbagGroceries(self):
        self.inputWorkspaceName = self.getPropertyValue("Inputworkspace")
        self.outputWorkspaceName = self.getPropertyValue("OutputWorkspaceGroup")
        self.outputWorkspace = WorkspaceGroup()
        if mtd.doesExist(self.outputWorkspaceName):
            DeleteWorkspaces(list(mtd[self.outputWorkspaceName].getNames()))
        mtd.addOrReplace(self.outputWorkspaceName, self.outputWorkspace)

    def PyExec(self):
        peakType = self.getPropertyValue("PeakType")
        reducedPeakList = parse_raw_as(List[GroupPeakList], self.getPropertyValue("DetectorPeaks"))
        self.chopIngredients(reducedPeakList)
        self.unbagGroceries()

        outputNames = [None] * len(FitOutputEnum)
        for x in FitOutputEnum:
            outputNames[x.value] = f"{self.outputWorkspaceName}{self.outputSuffix[x.value]}"

        for index, groupID in enumerate(self.groupIDs):
            outputNamesTmp = [None] * len(FitOutputEnum)
            for x in FitOutputEnum:
                outputNamesTmp[x.value] = f"{self.outputWorkspaceName}{self.outputSuffix[x.value]}_{index}"

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
                # in common with PDCalibration
                InputWorkspace="ws2fit",
                PeakFunction=peakType,
                PeakCenters=",".join(np.array(peakCenters).astype("str")),
                FitWindowBoundaryList=",".join(np.array(peakLimits).astype("str")),
                BackgroundType="Linear",
                MinimumSignalToNoiseRatio=self.NOYZE_2_MIN,
                ConstrainPeakPositions=True,
                HighBackground=True,  # vanadium must use high background
                # outputs -- in PDCalibration combined in workspace group
                FittedPeaksWorkspace=outputNamesTmp[FitOutputEnum.Workspace.value],
                OutputWorkspace=outputNamesTmp[FitOutputEnum.PeakPosition.value],
                OutputPeakParametersWorkspace=outputNamesTmp[FitOutputEnum.Parameters.value],
                OutputParameterFitErrorsWorkspace=outputNamesTmp[FitOutputEnum.ParameterError.value],
            )
            if index == 0:
                self.cloneWorkspaces(outputNamesTmp, outputNames)
            else:
                self.conjoinWorkspaces(outputNames, outputNamesTmp)
            self.mantidSnapper.WashDishes(
                "Deleting fitting workspace...",
                Workspace="ws2fit",
            )

        self.mantidSnapper.executeQueue()
        for output in outputNames:
            self.outputWorkspace.add(output)

        self.mantidSnapper.executeQueue()
        self.setProperty("OutputWorkspaceGroup", self.outputWorkspace.name())

    def cloneWorkspaces(self, inputs: List[str], outputs: List[str]):
        self.mantidSnapper.RenameWorkspaces(
            "Copying tmp workspace data",
            InputWorkspaces=inputs,
            WorkspaceNames=outputs,
        )

    def conjoinWorkspaces(self, input1: List[str], input2: List[str]):
        # combine the matrix workspaces
        for x in [FitOutputEnum.Workspace.value, FitOutputEnum.PeakPosition.value]:
            self.mantidSnapper.ConjoinWorkspaces(
                "Conjoin peak position workspaces",
                InputWorkspace1=input1[x],
                InputWorkspace2=input2[x],
                CheckOverlapping=False,
            )
            self.mantidSnapper.WashDishes(
                "Clear temporary workspace",
                Workspace=input2[x],
            )
        # combine the table workspaces
        for x in [FitOutputEnum.Parameters.value, FitOutputEnum.ParameterError.value]:
            self.mantidSnapper.ConjoinTableWorkspaces(
                "Conjoin peak fit parameter workspaces",
                InputWorkspace1=input1[x],
                InputWorkspace2=input2[x],
                AutoDelete=True,
            )


AlgorithmFactory.subscribe(FitMultiplePeaksAlgorithm)
