import json
from typing import List

import numpy as np
from mantid.api import AlgorithmFactory, PythonAlgorithm
from mantid.kernel import Direction
from pydantic import parse_raw_as

from snapred.backend.dao.CrystallographicInfo import CrystallographicInfo
from snapred.backend.dao.state.FocusGroup import FocusGroup
from snapred.backend.dao.state.InstrumentState import InstrumentState

name = "PurgeOverlappingPeaksAlgorithm"


class PurgeOverlappingPeaksAlgorithm(PythonAlgorithm):
    def PyInit(self):
        # declare properties
        self.declareProperty("InstrumentState", defaultValue="", direction=Direction.Input)
        self.declareProperty("FocusGroups", defaultValue="", direction=Direction.Input)
        self.declareProperty("CrystalInfo", defaultValue="", direction=Direction.Input)
        self.declareProperty("OutputPeakMap", defaultValue="", direction=Direction.Output)
        self.setRethrows(True)

    def PyExec(self):
        instrumentState = InstrumentState.parse_raw(self.getProperty("InstrumentState").value)
        focusGroups = parse_raw_as(List[FocusGroup], self.getProperty("FocusGroups").value)
        crystalInfo = CrystallographicInfo.parse_raw(self.getProperty("CrystalInfo").value)
        inputPeakList = crystalInfo.dSpacing
        # inputPeakList.sort()
        peakTuples = [
            (hkl_, dSpacing_, fSquared_, multiplicity_)
            for hkl_, dSpacing_, fSquared_, multiplicity_ in zip(
                crystalInfo.hkl, crystalInfo.dSpacing, crystalInfo.fSquared, crystalInfo.multiplicities
            )
        ]
        # sort by dSpacing
        peakTuples.sort(key=lambda x: x[1])
        inputPeakList = [dSpacing_ for hkl_, dSpacing_, fSquared_, multiplicity_ in peakTuples]
        beta_0 = instrumentState.gsasParameters.beta[0]
        beta_1 = instrumentState.gsasParameters.beta[1]
        FWHMMultiplierLeft = instrumentState.fwhmMultiplierLimit.minimum
        FWHMMultiplierRight = instrumentState.fwhmMultiplierLimit.maximum
        L = instrumentState.instrumentConfig.L1 + instrumentState.instrumentConfig.L2

        fSquared = np.array([peakTuple[2] for peakTuple in peakTuples])
        multiplicity = np.array([peakTuple[3] for peakTuple in peakTuples])
        dSpacing = np.array([peakTuple[1] for peakTuple in peakTuples])
        A = fSquared * multiplicity * dSpacing**4
        minimumA = np.max(A) * 0.05

        outputPeaks = {}
        for focIndex, focGroup in enumerate(focusGroups):
            delDoD = instrumentState.pixelGroupingInstrumentState[focIndex].delta_dhkl_over_dhkl
            tTheta = instrumentState.pixelGroupingInstrumentState[focIndex].twoThetaAverage

            dMin = instrumentState.pixelGroupingInstrumentState[focIndex].dhkl.minimum
            dMax = instrumentState.pixelGroupingInstrumentState[focIndex].dhkl.maximum

            # find index of 1.2460922059340045
            testIndex = inputPeakList.index(1.2460922059340045)
            crystalInfo.fSquared[testIndex] * crystalInfo.multiplicities[testIndex] * inputPeakList[testIndex] ** 4
            peakList = [
                peak
                for i, peak in enumerate(inputPeakList)
                if peakTuples[i][2] * peakTuples[i][3] * peakTuples[i][1] ** 4 >= minimumA
            ]
            peakList = [peak for peak in peakList if dMin <= peak <= dMax]
            nPks = len(peakList)
            keep = [True for i in range(nPks)]
            # import pdb; pdb.set_trace()

            outputPeakList = []
            for i in range(nPks - 1):
                if keep[i]:
                    peakSeparation = peakList[i + 1] - peakList[i]
                    # beta terms
                    beta_T = beta_0 + beta_1 / peakList[i] ** 4  # GSAS-I beta
                    beta_d = 505.548 * L * np.sin(tTheta / 2) * beta_T  # converted to d-space
                    #

                    fwhm1 = 2.35 * delDoD * peakList[i]
                    limRight = fwhm1 * FWHMMultiplierRight + 1 / beta_d
                    fwhm2 = 2.35 * delDoD * peakList[i + 1]
                    limLeft = fwhm2 * FWHMMultiplierLeft
                    # if round(peakList[i+1]) == round(3.13592995):
                    #     import pdb; pdb.set_trace()

                    if ((limRight + limLeft) >= peakSeparation) and (peakList[i + 1] != inputPeakList[i]):
                        keep[i] = False
                        keep[i + 1] = False
                    else:
                        outputPeakList.append(peakList[i])
            # and add last peak:
            if len(peakList) > 0:
                outputPeakList.append(peakList[-1])  # TODO: add check that DMAx is sufficient

            self.log().notice(f" {nPks} peaks in and {len(outputPeakList)} peaks out")
            outputPeaks[focGroup.name] = outputPeakList

        self.setProperty("OutputPeakMap", json.dumps(outputPeaks))

        return outputPeaks


AlgorithmFactory.subscribe(PurgeOverlappingPeaksAlgorithm)
