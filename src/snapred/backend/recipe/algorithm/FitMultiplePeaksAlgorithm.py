import json

import numpy as np
from mantid.api import AlgorithmFactory, PythonAlgorithm, WorkspaceGroup, mtd
from mantid.kernel import Direction

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

        beta_0 = instrumentState.gsasParameters.beta[0]
        beta_1 = instrumentState.gsasParameters.beta[1]
        FWHMMultiplierLeft = instrumentState.fwhmMultiplierLimit.minimum
        FWHMMultiplierRight = instrumentState.fwhmMultiplierLimit.maximum
        L = instrumentState.instrumentConfig.L1 + instrumentState.instrumentConfig.L2
        ws = self.mantidSnapper.mtd[wsName]
        # TODO: Fix this to use MantidSnapper when possible
        # Currently MantidSnapper is unable to return the List
        numSpec = ws.getNumberHistograms()
        purgeAlgo = PurgeOverlappingPeaksAlgorithm()
        purgeAlgo.initialize()
        purgeAlgo.setProperty("InstrumentState", instrumentState.json())
        purgeAlgo.setProperty("NumFocusGroups", str(numSpec))
        purgeAlgo.setProperty("CrystalInfo", crystalInfo.json())
        purgeAlgo.execute()
        reducedList = json.loads(purgeAlgo.getProperty("OutputPeakMap").value)

        ws_group = WorkspaceGroup()
        mtd.add("fitPeaksWSGroup", ws_group)

        for index in range(numSpec):
            fittedPeakPos = f"{wsName}_fitted_peakpositions_{index}"
            fittedParams = f"{wsName}_fitted_params_{index}"
            fittedWS = f"{wsName}_fitted_{index}"
            fittedParamsErr = f"{wsName}_fitted_params_err_{index}"
            delDoD = instrumentState.pixelGroupingInstrumentParameters[index].dRelativeResolution
            tTheta = instrumentState.pixelGroupingInstrumentParameters[index].twoTheta
            peakLimits = []
            for peak, dspc in enumerate(reducedList[index]):
                halfWindLeft = 2.35 * delDoD * dspc * FWHMMultiplierLeft
                halfWindRight = 2.35 * delDoD * dspc * FWHMMultiplierRight
                lowerLimit = dspc - halfWindLeft

                beta_t = beta_0 + beta_1 / dspc**4
                beta_d = 505.548 * L * np.sin(tTheta / 2) * beta_t
                upperLimit = dspc + halfWindRight + (1 / beta_d)
                peakLimits.extend([lowerLimit, upperLimit])

            self.mantidSnapper.ExtractSingleSpectrum(
                "Extract Single Spectrm...", InputWorkspace=wsName, OutputWorkspace="ws2fit", WorkspaceIndex=index
            )

            self.mantidSnapper.FitPeaks(
                "Fit Peaks...",
                InputWorkspace="ws2fit",
                PeakCenters=",".join(np.array(reducedList[index]).astype("str")),
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
