import json

import numpy as np
from mantid.api import AlgorithmFactory, PythonAlgorithm, WorkspaceGroup, mtd
from mantid.kernel import Direction

from snapred.backend.dao.DetectorPeak import DetectorPeak
from snapred.backend.dao.FitMultiplePeaksIngredients import FitMultiplePeaksIngredients
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper
from snapred.backend.recipe.algorithm.PurgeOverlappingPeaksAlgorithm import PurgeOverlappingPeaksAlgorithm

name = "FitMultiplePeaksAlgorithm"


class FitMultiplePeaksAlgorithm(PythonAlgorithm):
    def PyInit(self):
        # declare properties
        self.declareProperty("FitMultiplePeaksIngredients", defaultValue="", direction=Direction.Input)
        self.declareProperty("OutputWorkspaceGroup", defaultValue="", direction=Direction.Output)
        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, name)

    def PyExec(self):
        fitPeakIngredients = FitMultiplePeaksIngredients(
            **json.loads(self.getProperty("FitMultiplePeaksIngredients").value)
        )
        wsName = fitPeakIngredients.InputWorkspace
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
        for index in range(ws.getNumberHistograms()):
            fittedPeakPos = f"{wsName}_fitted_peakpositions_{index}"
            fittedParams = f"{wsName}_fitted_params_{index}"
            fittedWS = f"{wsName}_fitted_{index}"
            fittedParamsErr = f"{wsName}_fitted_params_err_{index}"

            peakCenters = []
            peakLimits = []
            for peak in reducedList[index]:
                peakCenters.append(peak.position.value)
                peakLimits.extend([peak.position.minimum, peak.position.maximum])

            self.mantidSnapper.ExtractSingleSpectrum(
                "Extract Single Spectrm...", InputWorkspace=wsName, OutputWorkspace="ws2fit", WorkspaceIndex=index
            )

            self.mantidSnapper.FitPeaks(
                "Fit Peaks...",
                InputWorkspace="ws2fit",
                PeakCenters=",".join(np.array(peakCenters).astype("str")),
                PeakFunction=peakType,
                FitWindowBoundaryList=",".join(np.array(peakLimits).astype("str")),
                OutputWorkspace=fittedPeakPos,
                OutputPeakParametersWorkspace=fittedParams,
                BackgroundType="Quadratic",
                FittedPeaksWorkspace=fittedWS,
                ConstrainPeakPositions=True,
                OutputParameterFitErrorsWorkspace=fittedParamsErr,
            )
            self.mantidSnapper.executeQueue()
            ws_group.add(fittedPeakPos)
            ws_group.add(fittedParams)
            ws_group.add(fittedWS)
            ws_group.add(fittedParamsErr)

        self.mantidSnapper.DeleteWorkspace(
            "Deleting fitting workspace...",
            Workspace="ws2fit",
        )
        self.mantidSnapper.executeQueue()
        self.setProperty("OutputWorkspaceGroup", ws_group.name())
        return ws_group


AlgorithmFactory.subscribe(FitMultiplePeaksAlgorithm)
