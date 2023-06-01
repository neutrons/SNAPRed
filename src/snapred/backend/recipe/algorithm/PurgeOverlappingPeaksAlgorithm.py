from typing import List

import numpy as np
from mantid.api import AlgorithmFactory, PythonAlgorithm
from mantid.kernel import Direction
from pydantic import parse_raw_as

from snapred.backend.dao.state.FocusGroup import FocusGroup
from snapred.backend.dao.state.InstrumentState import InstrumentState

name = "PurgeOverlappingPeaksAlgorithm"


class PurgeOverlappingPeaksAlgorithm(PythonAlgorithm):
    def PyInit(self):
        # declare properties
        self.declareProperty("InstrumentState", defaultValue="", direction=Direction.Input)
        self.declareProperty("FocusGroups", defaultValue="", direction=Direction.Input)
        self.declareProperty("PeakList", defaultValue="", direction=Direction.Input)
        self.declareProperty("OutputPeakMap", defaultValue="", direction=Direction.Output)
        self.setRethrows(True)

    def PyExec(self):
        instrumentState = InstrumentState.parse_raw(self.getProperty("InstrumentState").value)
        focusGroups = parse_raw_as(List[FocusGroup], self.getProperty("FocusGroups").value)
        inputPeakList = self.getProperty("PeakList").value
        beta_0 = instrumentState.gsasParameters.beta[0]
        beta_1 = instrumentState.gsasParameters.beta[1]
        FWHMMultiplierLeft = instrumentState.FWHMMultiplierLimit.minimum
        FWHMMultiplierRight = instrumentState.FWHMMultiplierLimit.maximum
        L = instrumentState.instrumentConfig.L1 + instrumentState.instrumentConfig.L2

        outputPeaks = {}
        for focIndex, focGroup in enumerate(focusGroups):
            delDoD = instrumentState.pixelGroupingInstrumentState[focIndex].delta_dhkl_over_dhkl
            tTheta = instrumentState.pixelGroupingInstrumentState[focIndex].twoThetaAverage
            nPks = len(inputPeakList)

            keep = [True for i in range(nPks)]

            outputPeakList = []
            for i in range(nPks - 1):
                if keep[i]:
                    peakSeparation = inputPeakList[i + 1] - inputPeakList[i]

                    # beta terms
                    beta_T = beta_0 + beta_1 / inputPeakList[i] ** 4  # GSAS-I beta
                    beta_d = 505.548 * L * np.sin(tTheta / 2) * beta_T  # converted to d-space
                    #

                    fwhm1 = 2.35 * delDoD * inputPeakList[i]
                    limRight = fwhm1 * FWHMMultiplierRight + 1 / beta_d
                    fwhm2 = 2.35 * delDoD * inputPeakList[i + 1]
                    limLeft = fwhm2 * FWHMMultiplierLeft

                    if ((limRight + limLeft) >= peakSeparation) and (inputPeakList[i + 1] != inputPeakList[i]):
                        keep[i] = False
                        keep[i + 1] = False
                    else:
                        outputPeakList.append(inputPeakList[i])
            # and add last peak:
            outputPeakList.append(inputPeakList[-1])  # TODO: add check that DMAx is sufficient

            self.log().notice(f" {nPks} peaks in and {len(outputPeakList)} peaks out")
            outputPeaks[focGroup.name] = outputPeakList

        self.setProperty("OutputPeakMap", outputPeaks)

        return outputPeaks


AlgorithmFactory.subscribe(PurgeOverlappingPeaksAlgorithm)
