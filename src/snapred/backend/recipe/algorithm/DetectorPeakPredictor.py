import json

import numpy as np
from mantid.api import AlgorithmFactory, PythonAlgorithm
from mantid.kernel import Direction, PhysicalConstants

from snapred.backend.dao.DetectorPeak import DetectorPeak
from snapred.backend.dao.GroupPeakList import GroupPeakList
from snapred.backend.dao.ingredients import PeakIngredients
from snapred.backend.dao.Limit import LimitedValue
from snapred.meta.Config import Config
from snapred.meta.redantic import list_to_raw


class DetectorPeakPredictor(PythonAlgorithm):
    # TODO: Change BETA_D_COEFFICIENT to
    # BETA_D_COEFFICIENT = PhysicalConstants.h / (2 * PhysicalConstants.NeutronMass * 1e10)
    # also the equation below that uses the coefficient should be changed to
    # beta_d = BETA_D_COEFFICIENT*beta_T/(L*np.sin(tTheta/2))
    BETA_D_COEFFICIENT = 1 / (PhysicalConstants.h / (2 * PhysicalConstants.NeutronMass) * Config["constants.m2cm"])
    FWHM = Config["constants.DetectorPeakPredictor.fwhm"]
    PEAK_INTENSITY_THRESHOLD = Config["constants.PeakIntensityFractionThreshold"]

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
        groupIDs = ingredients.pixelGroup.groupIDs
        self.delDoD = dict(zip(groupIDs, ingredients.pixelGroup.dRelativeResolution))
        self.tTheta = dict(zip(groupIDs, ingredients.pixelGroup.twoTheta))
        self.dMin = dict(zip(groupIDs, ingredients.pixelGroup.dMin()))
        self.dMax = dict(zip(groupIDs, ingredients.pixelGroup.dMax()))

        # select only peaks above the amplitude threshold
        crystalInfo = ingredients.crystalInfo
        crystalInfo.peaks.sort(key=lambda x: x.dSpacing)
        fSquared = np.array(crystalInfo.fSquared)
        multiplicity = np.array(crystalInfo.multiplicities)
        dSpacing = np.array(crystalInfo.dSpacing)

        if fSquared.size == 0 or multiplicity.size == 0 or dSpacing.size == 0:
            A = np.array([])
        else:
            A = fSquared * multiplicity * dSpacing**4

        thresholdA = 0
        self.goodPeaks = []

        if A.size > 0:
            thresholdA = np.max(A) * ingredients.peakIntensityThreshold
            self.goodPeaks = [peak for i, peak in enumerate(crystalInfo.peaks) if A[i] >= thresholdA]

        self.allGroupIDs = ingredients.pixelGroup.groupIDs

    def PyExec(self) -> None:
        ingredients = PeakIngredients.parse_raw(self.getProperty("Ingredients").value)
        self.chopIngredients(ingredients)

        allFocusGroupsPeaks = []
        for groupID in self.allGroupIDs:
            initialPeakCount = len(self.goodPeaks)
            self.log().notice(f"Initial peak count for group {groupID}: {initialPeakCount}")

            # Apply dMin filtering
            dList = [peak.dSpacing for peak in self.goodPeaks if self.dMin[groupID] <= peak.dSpacing]

            filteredPeakCount = len(dList)
            self.log().notice(
                f"Filtered peak count for group {groupID} after applying dMin={self.dMin[groupID]}: {filteredPeakCount}"
            )

            singleFocusGroupPeaks = []
            for d in dList:
                # Compute beta terms and other calculations
                beta_T = self.beta_0 + self.beta_1 / d**4  # GSAS-I beta
                beta_d = self.BETA_D_COEFFICIENT * self.L * np.sin(self.tTheta[groupID] / 2) * beta_T

                fwhm = self.FWHM * self.delDoD[groupID] * d
                widthLeft = fwhm * self.FWHMMultiplierLeft
                widthRight = fwhm * self.FWHMMultiplierRight + self.peakTailCoefficient / beta_d

                singleFocusGroupPeaks.append(
                    DetectorPeak(position=LimitedValue(value=d, minimum=d - widthLeft, maximum=d + widthRight))
                )

            # Check if no peaks were added to singleFocusGroupPeaks
            if not singleFocusGroupPeaks:
                maxFwhm = 0  # Set maxFwhm to 0 or another appropriate default value
                singleFocusGroupPeakList = GroupPeakList(peaks=[], groupID=groupID, maxfwhm=maxFwhm)  # No peaks to add
            else:
                maxFwhm = self.FWHM * max(dList, default=0.0) * self.delDoD[groupID]
                singleFocusGroupPeakList = GroupPeakList(peaks=singleFocusGroupPeaks, groupID=groupID, maxfwhm=maxFwhm)

            self.log().notice(f"Focus group {groupID} : {len(singleFocusGroupPeaks)} peaks out")
            allFocusGroupsPeaks.append(singleFocusGroupPeakList)

        self.setProperty("DetectorPeaks", list_to_raw(allFocusGroupsPeaks))
        return allFocusGroupsPeaks


AlgorithmFactory.subscribe(DetectorPeakPredictor)
