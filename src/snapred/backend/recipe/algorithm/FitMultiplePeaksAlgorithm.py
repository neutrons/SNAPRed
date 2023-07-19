import json
from enum import Enum

import numpy as np
from mantid.api import AlgorithmFactory, PythonAlgorithm, WorkspaceFactory, WorkspaceGroup, mtd
from mantid.kernel import Direction

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
        outputeWsName = self.getProperty("OutputWorkspaceGroup").value
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
        mtd.add(outputeWsName, ws_group)

        for subgroupIndex in range(numSpec):
            outputNames = [None for _ in range(len(FitOutputEnum))]
            outputNames[FitOutputEnum.PeakPosition.value] = f"{wsName}_fitted_peakpositions_{subgroupIndex}"
            outputNames[FitOutputEnum.Parameters.value] = f"{wsName}_fitted_params_{subgroupIndex}"
            outputNames[FitOutputEnum.Workspace.value] = f"{wsName}_fitted_{subgroupIndex}"
            outputNames[FitOutputEnum.ParameterError.value] = f"{wsName}_fitted_params_err_{subgroupIndex}"
            delDoD = instrumentState.pixelGroupingInstrumentParameters[subgroupIndex].dRelativeResolution
            tTheta = instrumentState.pixelGroupingInstrumentParameters[subgroupIndex].twoTheta
            peakLimits = []
            for peak, dspc in enumerate(reducedList[subgroupIndex]):
                halfWindLeft = 2.35 * delDoD * dspc * FWHMMultiplierLeft
                halfWindRight = 2.35 * delDoD * dspc * FWHMMultiplierRight
                lowerLimit = dspc - halfWindLeft

                beta_t = beta_0 + beta_1 / dspc**4
                beta_d = 505.548 * L * np.sin(tTheta / 2) * beta_t
                upperLimit = dspc + halfWindRight + (1 / beta_d)
                peakLimits.extend([lowerLimit, upperLimit])
                # strain = dref-dobserved / sigma

            self.mantidSnapper.ExtractSingleSpectrum(
                "Extract Single Spectrm...",
                InputWorkspace=wsName,
                OutputWorkspace="ws2fit",
                WorkspaceIndex=subgroupIndex,
            )

            self.mantidSnapper.FitPeaks(
                "Fit Peaks...",
                InputWorkspace="ws2fit",
                PeakCenters=",".join(np.array(reducedList[subgroupIndex]).astype("str")),
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
