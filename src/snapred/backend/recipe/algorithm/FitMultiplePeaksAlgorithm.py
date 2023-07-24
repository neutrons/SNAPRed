import json
from enum import Enum

import numpy as np
from mantid.api import AlgorithmFactory, PythonAlgorithm, WorkspaceFactory, WorkspaceGroup, mtd
from mantid.kernel import Direction

from snapred.backend.dao.DetectorPeak import DetectorPeak
from snapred.backend.dao.FitMultiplePeaksIngredients import FitMultiplePeaksIngredients
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper
from snapred.backend.recipe.algorithm.PurgeOverlappingPeaksAlgorithm import PurgeOverlappingPeaksAlgorithm

name = "FitMultiplePeaksAlgorithm"


class FitOutputEnum(Enum):
    PeakPosition = 0
    Parameters = 1
    Workspace = 2
    ParameterError = 3


class FitMultiplePeaksAlgorithm(PythonAlgorithm):
    def PyInit(self):
        # declare properties
        self.declareProperty("FitMultiplePeaksIngredients", defaultValue="", direction=Direction.Input)
        self.declareProperty("OutputWorkspaceGroup", defaultValue="fitPeaksWSGroup", direction=Direction.Output)
        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, name)

    def listToWorkspace(self, aList, name):
        ws = WorkspaceFactory.create("Workspace2D", NVectors=1, XLength=len(aList), YLength=len(aList))
        ws.setX(0, np.asarray(aList))
        # register ws in mtd
        mtd.add(name, ws)
        return ws

    def PyExec(self):
        fitPeakIngredients = FitMultiplePeaksIngredients(
            **json.loads(self.getProperty("FitMultiplePeaksIngredients").value)
        )
        wsName = fitPeakIngredients.InputWorkspace
        self.getProperty("OutputWorkspaceGroup").value
        instrumentState = fitPeakIngredients.InstrumentState
        crystalInfo = fitPeakIngredients.CrystalInfo
        peakType = fitPeakIngredients.PeakType

        result = self.mantidSnapper.PurgeOverlappingPeaksAlgorithm(
            "Purging overlapping peaks...", InstrumentState=instrumentState.json(), CrystalInfo=crystalInfo.json()
        )
        self.mantidSnapper.executeQueue()
        reducedList_json = json.loads(result.get())

        reducedList = [
            [DetectorPeak.parse_raw(peak_json) for peak_json in peak_group_json] for peak_group_json in reducedList_json
        ]

        ws_group = WorkspaceGroup()
        mtd.add("fitPeaksWSGroup", ws_group)

        ws = self.mantidSnapper.mtd[wsName]
        for subgroupIndex in range(ws.getNumberHistograms()):
            outputNames = [None for _ in range(len(FitOutputEnum))]
            outputNames[FitOutputEnum.PeakPosition.value] = f"{wsName}_fitted_peakpositions_{subgroupIndex}"
            outputNames[FitOutputEnum.Parameters.value] = f"{wsName}_fitted_params_{subgroupIndex}"
            outputNames[FitOutputEnum.Workspace.value] = f"{wsName}_fitted_{subgroupIndex}"
            outputNames[FitOutputEnum.ParameterError.value] = f"{wsName}_fitted_params_err_{subgroupIndex}"

            peakCenters = []
            peakLimits = []
            for peak in reducedList[subgroupIndex]:
                peakCenters.append(peak.position.value)
                peakLimits.extend([peak.position.minimum, peak.position.maximum])

            self.mantidSnapper.ExtractSingleSpectrum(
                "Extract Single Spectrm...",
                InputWorkspace=wsName,
                OutputWorkspace="ws2fit",
                WorkspaceIndex=subgroupIndex,
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
                ws_group.add(output)

        self.mantidSnapper.DeleteWorkspace(
            "Deleting fitting workspace...",
            Workspace="ws2fit",
        )
        self.mantidSnapper.executeQueue()
        self.setProperty("OutputWorkspaceGroup", ws_group.name())
        return ws_group


AlgorithmFactory.subscribe(FitMultiplePeaksAlgorithm)
