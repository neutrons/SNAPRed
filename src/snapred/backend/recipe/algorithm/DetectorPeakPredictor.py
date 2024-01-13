import json

import numpy as np
from mantid.api import AlgorithmFactory, PythonAlgorithm
from mantid.kernel import Direction

from snapred.backend.dao.DetectorPeak import DetectorPeak
from snapred.backend.dao.GroupPeakList import GroupPeakList
from snapred.backend.dao.ingredients import PeakIngredients
from snapred.backend.dao.Limit import LimitedValue
from snapred.meta.redantic import list_to_raw


class DetectorPeakPredictor(PythonAlgorithm):
    def category(self):
        return "SNAPRed Data Processing"

    def PyInit(self) -> None:
        # declare properties
        self.declareProperty(
            "Ingredients",
            defaultValue="",
            direction=Direction.Input,
            doc="The detector peak ingredients",
        )
        self.declareProperty(
            "DetectorPeaks",
            defaultValue="",
            direction=Direction.Output,
            doc="The returned list of GroupPeakList objects",
        )
        self.setRethrows(True)

    def chopIngredients(self, ingredients: PeakIngredients) -> None:
        self.beta_0 = ingredients.instrumentState.gsasParameters.beta[0]
        self.beta_1 = ingredients.instrumentState.gsasParameters.beta[1]
        self.FWHMMultiplierLeft = ingredients.instrumentState.fwhmMultiplierLimit.minimum
        self.FWHMMultiplierRight = ingredients.instrumentState.fwhmMultiplierLimit.maximum
        self.peakTailCoefficient = ingredients.instrumentState.peakTailCoefficient
        self.L = ingredients.instrumentState.instrumentConfig.L1 + ingredients.instrumentState.instrumentConfig.L2

        # binning params
        pixelGroupParams = ingredients.pixelGroup.pixelGroupingParameters
        self.delDoD = {groupID: pgp.dRelativeResolution for groupID, pgp in pixelGroupParams.items()}
        self.tTheta = {groupID: pgp.twoTheta for groupID, pgp in pixelGroupParams.items()}
        self.dMin = {groupID: pgp.dResolution.minimum for groupID, pgp in pixelGroupParams.items()}
        self.dMax = {groupID: pgp.dResolution.maximum for groupID, pgp in pixelGroupParams.items()}

        # select only peaks above the amplitude threshold
        crystalInfo = ingredients.crystalInfo
        crystalInfo.peaks.sort(key=lambda x: x.dSpacing)
        fSquared = np.array(crystalInfo.fSquared)
        multiplicity = np.array(crystalInfo.multiplicities)
        dSpacing = np.array(crystalInfo.dSpacing)
        A = fSquared * multiplicity * dSpacing**4
        thresholdA = np.max(A) * ingredients.peakIntensityFractionalThreshold
        self.goodPeaks = [peak for i, peak in enumerate(crystalInfo.peaks) if A[i] >= thresholdA]

        self.allGroupIDs = ingredients.pixelGroup.groupIDs

    def PyExec(self) -> None:
        ingredients = PeakIngredients.parse_raw(self.getProperty("Ingredients").value)
        self.chopIngredients(ingredients)

        allFocusGroupsPeaks = []
        for groupID in self.allGroupIDs:
            # select only good peaks within the d-spacing range
            dList = [
                peak.dSpacing for peak in self.goodPeaks if self.dMin[groupID] <= peak.dSpacing <= self.dMax[groupID]
            ]

            singleFocusGroupPeaks = []
            for d in dList:
                # beta terms
                beta_T = self.beta_0 + self.beta_1 / d**4  # GSAS-I beta
                beta_d = 505.548 * self.L * np.sin(self.tTheta[groupID] / 2) * beta_T  # converted to d-space

                fwhm = 2.35 * self.delDoD[groupID] * d
                widthLeft = fwhm * self.FWHMMultiplierLeft
                widthRight = fwhm * self.FWHMMultiplierRight + self.peakTailCoefficient / beta_d

                singleFocusGroupPeaks.append(
                    DetectorPeak(position=LimitedValue(value=d, minimum=d - widthLeft, maximum=d + widthRight))
                )
            maxFwhm = 2.35 * max(dList, default=0.0) * self.delDoD[groupID]

            singleFocusGroupPeakList = GroupPeakList(peaks=singleFocusGroupPeaks, groupID=groupID, maxfwhm=maxFwhm)
            self.log().notice(f"Focus group {groupID} : {len(dList)} peaks out")
            allFocusGroupsPeaks.append(singleFocusGroupPeakList)

        self.setProperty("DetectorPeaks", list_to_raw(allFocusGroupsPeaks))
        return allFocusGroupsPeaks


AlgorithmFactory.subscribe(DetectorPeakPredictor)
