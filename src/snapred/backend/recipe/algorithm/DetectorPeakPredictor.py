import json

import numpy as np
from mantid.api import AlgorithmFactory, PythonAlgorithm
from mantid.kernel import Direction

from snapred.backend.dao.CrystallographicInfo import CrystallographicInfo
from snapred.backend.dao.DetectorPeak import DetectorPeak
from snapred.backend.dao.Limit import LimitedValue
from snapred.backend.dao.state.InstrumentState import InstrumentState

name = "DetectorPeakPredictor"


class DetectorPeakPredictor(PythonAlgorithm):
    def PyInit(self):
        # declare properties
        self.declareProperty("InstrumentState", defaultValue="", direction=Direction.Input)
        self.declareProperty("CrystalInfo", defaultValue="", direction=Direction.Input)
        self.declareProperty("DetectorPeaks", defaultValue="", direction=Direction.Output)
        self.setRethrows(True)

    def PyExec(self):
        instrumentState = InstrumentState.parse_raw(self.getProperty("InstrumentState").value)
        crystalInfo = CrystallographicInfo.parse_raw(self.getProperty("CrystalInfo").value)
        crystalInfo.peaks.sort(key=lambda x: x.dSpacing)
        beta_0 = instrumentState.gsasParameters.beta[0]
        beta_1 = instrumentState.gsasParameters.beta[1]
        FWHMMultiplierLeft = instrumentState.fwhmMultiplierLimit.minimum
        FWHMMultiplierRight = instrumentState.fwhmMultiplierLimit.maximum
        peakTailCoefficient = instrumentState.peakTailCoefficient
        L = instrumentState.instrumentConfig.L1 + instrumentState.instrumentConfig.L2

        fSquared = np.array(crystalInfo.fSquared)
        multiplicity = np.array(crystalInfo.multiplicities)
        dSpacing = np.array(crystalInfo.dSpacing)
        A = fSquared * multiplicity * dSpacing**4
        thresholdA = np.max(A) * 0.05

        allFocusGroupsPeaks = []
        numFocusGroups = len(instrumentState.pixelGroupingInstrumentParameters)
        for index in range(numFocusGroups):
            delDoD = instrumentState.pixelGroupingInstrumentParameters[index].dRelativeResolution
            tTheta = instrumentState.pixelGroupingInstrumentParameters[index].twoTheta

            dMin = instrumentState.pixelGroupingInstrumentParameters[index].dResolution.minimum
            dMax = instrumentState.pixelGroupingInstrumentParameters[index].dResolution.maximum

            dList = [peak.dSpacing for i, peak in enumerate(crystalInfo.peaks) if A[i] >= thresholdA]
            dList = [d for d in dList if dMin <= d <= dMax]

            singleFocusGroupPeaks = []
            for d in dList:
                # beta terms
                beta_T = beta_0 + beta_1 / d**4  # GSAS-I beta
                beta_d = 505.548 * L * np.sin(tTheta / 2) * beta_T  # converted to d-space

                fwhm = 2.35 * delDoD * d
                widthLeft = fwhm * FWHMMultiplierLeft
                widthRight = fwhm * FWHMMultiplierRight + peakTailCoefficient / beta_d

                singleFocusGroupPeaks.append(
                    DetectorPeak(position=LimitedValue(value=d, minimum=d - widthLeft, maximum=d + widthRight)).json()
                )

            self.log().notice(f"Focus group {index} : {len(dList)} peaks out")
            allFocusGroupsPeaks.append(singleFocusGroupPeaks)

        self.setProperty("DetectorPeaks", json.dumps(allFocusGroupsPeaks))

        return allFocusGroupsPeaks


AlgorithmFactory.subscribe(DetectorPeakPredictor)
