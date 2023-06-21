import json
from typing import List

import numpy as np
from mantid.api import AlgorithmFactory, PythonAlgorithm
from mantid.kernel import Direction

from snapred.backend.dao.CrystallographicInfo import CrystallographicInfo
from snapred.backend.dao.state.InstrumentState import InstrumentState

name = "PurgeOverlappingPeaksAlgorithm"


class PurgeOverlappingPeaksAlgorithm(PythonAlgorithm):
    def PyInit(self):
        # declare properties
        self.declareProperty("InstrumentState", defaultValue="", direction=Direction.Input)
        self.declareProperty("CrystalInfo", defaultValue="", direction=Direction.Input)
        self.declareProperty("NumFocusGroups", defaultValue="", direction=Direction.Input)
        self.declareProperty("OutputPeakMap", defaultValue="", direction=Direction.Output)
        self.setRethrows(True)

    def PyExec(self):
        instrumentState = InstrumentState.parse_raw(self.getProperty("InstrumentState").value)
        numFocusGroups = int(self.getProperty("NumFocusGroups").value)
        crystalInfo = CrystallographicInfo.parse_raw(self.getProperty("CrystalInfo").value)
        crystalInfo.peaks.sort(key=lambda x: x.dSpacing)
        beta_0 = instrumentState.gsasParameters.beta[0]
        beta_1 = instrumentState.gsasParameters.beta[1]
        FWHMMultiplierLeft = instrumentState.fwhmMultiplierLimit.minimum
        FWHMMultiplierRight = instrumentState.fwhmMultiplierLimit.maximum
        L = instrumentState.instrumentConfig.L1 + instrumentState.instrumentConfig.L2

        fSquared = np.array(crystalInfo.fSquared)
        multiplicity = np.array(crystalInfo.multiplicities)
        dSpacing = np.array(crystalInfo.dSpacing)
        A = fSquared * multiplicity * dSpacing**4
        maxA = np.max(A) * 0.05

        outputPeaks = []

        for index in range(numFocusGroups):
            delDoD = instrumentState.pixelGroupingInstrumentState[index].delta_dhkl_over_dhkl
            tTheta = instrumentState.pixelGroupingInstrumentState[index].twoThetaAverage

            dMin = instrumentState.pixelGroupingInstrumentState[index].dhkl.minimum
            dMax = instrumentState.pixelGroupingInstrumentState[index].dhkl.maximum

            peakList = [peak.dSpacing for i, peak in enumerate(crystalInfo.peaks) if A[i] >= maxA]
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

                    if ((limRight + limLeft) >= peakSeparation) and (peakList[i + 1] != peakList[i]):
                        keep[i] = False
                        keep[i + 1] = False
                    else:
                        outputPeakList.append(peakList[i])
            # and add last peak:
            if len(peakList) > 0:
                outputPeakList.append(peakList[-1])  # TODO: add check that DMAx is sufficient

            self.log().notice(f" {nPks} peaks in and {len(outputPeakList)} peaks out")
            outputPeaks.append(outputPeakList)

        self.setProperty("OutputPeakMap", json.dumps(outputPeaks))
        return outputPeaks


AlgorithmFactory.subscribe(PurgeOverlappingPeaksAlgorithm)
