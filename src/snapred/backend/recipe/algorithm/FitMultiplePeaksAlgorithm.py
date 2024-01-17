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
from pydantic import parse_raw_as

from snapred.backend.dao.GroupPeakList import GroupPeakList
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper
from snapred.backend.recipe.algorithm.PurgeOverlappingPeaksAlgorithm import PurgeOverlappingPeaksAlgorithm
from snapred.meta.Config import Config

logger = snapredLogger.getLogger(__name__)


class FitOutputEnum(Enum):
    PeakPosition = 0
    Parameters = 1
    Workspace = 2
    ParameterError = 3


class FitMultiplePeaksAlgorithm(PythonAlgorithm):
    PEAK_INTENSITY_THRESHOLD = Config["constants.PeakIntensityFractionThreshold"]

    def category(self):
        return "SNAPRed Data Processing"

    def PyInit(self):
        allowed_peak_types = [
            "AsymmetricPearsonVII",
            "BackToBackExponential",
            "Bk2BkExpConvPV",
            "DeltaFunction",
            "ElasticDiffRotDiscreteCircle",
            "ElasticDiffSphere",
            "ElasticIsoRotDiff",
            "ExamplePeakFunction",
            "Gaussian",
            "IkedaCarpenterPV",
            "Lorentzian",
            "PseudoVoigt",
            "Voigt",
        ]
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
            "DetectorPeakIngredients",
            defaultValue="",
            direction=Direction.Input,
            doc="Ingredients for DetectorPeakPredictor to find the lsit of peaks",
        )
        self.declareProperty("PeakType", "Gaussian", StringListValidator(allowed_peak_types), direction=Direction.Input)
        self.declareProperty("OutputWorkspaceGroup", defaultValue="fitPeaksWSGroup", direction=Direction.Output)
        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, __name__)

    def listToWorkspace(self, aList, name):
        ws = WorkspaceFactory.create("Workspace2D", NVectors=1, XLength=len(aList), YLength=len(aList))
        ws.setX(0, np.asarray(aList))
        # register ws in mtd
        mtd.add(name, ws)
        return ws

    def validateInputs(self) -> Dict[str, str]:
        errors = {}
        waysToGetPeaks = ["DetectorPeaks", "DetectorPeakIngredients"]
        definedWaysToGetPeaks = [x for x in waysToGetPeaks if not self.getProperty(x).isDefault]
        if len(definedWaysToGetPeaks) == 0:
            msg = "Purse peaks requires either a list of peaks, or ingredients to detect peaks"
            errors["DetectorPeaks"] = msg
            errors["DetectorPeakIngredients"] = msg
        elif len(definedWaysToGetPeaks) == 2:
            logger.warn(
                """Both a list of detector peaks and ingredients were given;
                the list will be used and ingredients ignored"""
            )
        return errors

    def PyExec(self):
        inputWorkspaceName = self.getPropertyValue("Inputworkspace")
        outputWorkspaceName = self.getPropertyValue("OutputWorkspaceGroup")
        peakType = self.getPropertyValue("PeakType")

        peakString = self.mantidSnapper.PurgeOverlappingPeaksAlgorithm(
            "Purging overlapping peaks...",
            DetectorPeaks=self.getPropertyValue("DetectorPeaks"),
            DetectorPeakIngredients=self.getPropertyValue("DetectorPeakIngredients"),
        )
        self.mantidSnapper.executeQueue()
        reducedPeakList = parse_raw_as(List[GroupPeakList], peakString.get())

        outputWorkspace = WorkspaceGroup()
        mtd.add(outputWorkspaceName, outputWorkspace)

        groupIDs = []
        reducedList = {}
        for groupPeakList in reducedPeakList:
            groupIDs.append(groupPeakList.groupID)
            reducedList[groupPeakList.groupID] = groupPeakList.peaks

        for index, groupID in enumerate(groupIDs):
            outputNames = [None] * len(FitOutputEnum)
            outputNames[FitOutputEnum.PeakPosition.value] = f"{inputWorkspaceName}_fitted_peakpositions_{index}"
            outputNames[FitOutputEnum.Parameters.value] = f"{inputWorkspaceName}_fitted_params_{index}"
            outputNames[FitOutputEnum.Workspace.value] = f"{inputWorkspaceName}_fitted_{index}"
            outputNames[FitOutputEnum.ParameterError.value] = f"{inputWorkspaceName}_fitted_params_err_{index}"

            peakCenters = []
            peakLimits = []
            for peak in reducedList[groupID]:
                peakCenters.append(peak.position.value)
                peakLimits.extend([peak.position.minimum, peak.position.maximum])

            self.mantidSnapper.ExtractSingleSpectrum(
                "Extract Single Spectrum...",
                InputWorkspace=inputWorkspaceName,
                OutputWorkspace="ws2fit",
                WorkspaceIndex=index,
            )

            self.mantidSnapper.FitPeaks(
                "Fit Peaks...",
                InputWorkspace="ws2fit",
                PeakCenters=",".join(np.array(peakCenters).astype("str")),
                PeakFunction=peakType,
                FitWindowBoundaryList=",".join(np.array(peakLimits).astype("str")),
                OutputWorkspace=outputNames[FitOutputEnum.PeakPosition.value],
                OutputPeakParametersWorkspace=outputNames[FitOutputEnum.Parameters.value],
                BackgroundType="Quadratic",
                FittedPeaksWorkspace=outputNames[FitOutputEnum.Workspace.value],
                ConstrainPeakPositions=True,
                OutputParameterFitErrorsWorkspace=outputNames[FitOutputEnum.ParameterError.value],
            )
            self.mantidSnapper.executeQueue()
            for output in outputNames:
                outputWorkspace.add(output)

        self.mantidSnapper.WashDishes(
            "Deleting fitting workspace...",
            Workspace="ws2fit",
        )
        self.mantidSnapper.executeQueue()
        self.setProperty("OutputWorkspaceGroup", outputWorkspace.name())
        return outputWorkspace


AlgorithmFactory.subscribe(FitMultiplePeaksAlgorithm)
