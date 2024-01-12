import json
from enum import Enum

import numpy as np
from mantid.api import AlgorithmFactory, PythonAlgorithm, WorkspaceFactory, WorkspaceGroup, mtd
from mantid.kernel import Direction, StringListValidator

from snapred.backend.dao.GroupPeakList import GroupPeakList
from snapred.backend.dao.ingredients import PeakIngredients as Ingredients
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper
from snapred.backend.recipe.algorithm.PurgeOverlappingPeaksAlgorithm import PurgeOverlappingPeaksAlgorithm


class FitOutputEnum(Enum):
    PeakPosition = 0
    Parameters = 1
    Workspace = 2
    ParameterError = 3


class FitMultiplePeaksAlgorithm(PythonAlgorithm):
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
        self.declareProperty("InputWorkspace", defaultValue="", direction=Direction.Input)
        self.declareProperty("Ingredients", defaultValue="", direction=Direction.Input)
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

    def PyExec(self):
        ingredients = Ingredients.parse_raw(self.getPropertyValue("Ingredients"))
        inputWorkspaceName = self.getPropertyValue("Inputworkspace")
        outputWorkspaceName = self.getProperty("OutputWorkspaceGroup").value
        peakType = self.getPropertyValue("PeakType")

        result = self.mantidSnapper.PurgeOverlappingPeaksAlgorithm(
            "Purging overlapping peaks...",
            DetectorPeakIngredients=ingredients.json(),
        )
        self.mantidSnapper.executeQueue()
        reducedList_json = json.loads(result.get())

        outputWorkspace = WorkspaceGroup()
        mtd.add(outputWorkspaceName, outputWorkspace)

        groupIDs = []
        reducedList = {}
        for x in reducedList_json:
            groupPeakList = GroupPeakList.parse_obj(x)
            groupIDs.append(groupPeakList.groupID)
            reducedList[groupPeakList.groupID] = groupPeakList.peaks

        for index, groupID in enumerate(groupIDs):
            outputNames = [None for _ in range(len(FitOutputEnum))]
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
