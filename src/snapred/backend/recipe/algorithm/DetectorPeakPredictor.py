import numpy as np
from mantid.api import AlgorithmFactory, PythonAlgorithm
from mantid.kernel import Direction, PhysicalConstants
from mantid.simpleapi import _create_algorithm_function

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
        self.declareProperty(
            "PurgeDuplicates",
            defaultValue=True,
            direction=Direction.Input,
            doc="Purge duplicate peaks",
        )
        self.setRethrows(True)

    def chopIngredients(self, ingredients: PeakIngredients) -> None:
        self.beta_0 = ingredients.instrumentState.gsasParameters.beta[0]
        self.beta_1 = ingredients.instrumentState.gsasParameters.beta[1]
        self.FWHMMultiplierLeft = ingredients.instrumentState.fwhmMultipliers.left
        self.FWHMMultiplierRight = ingredients.instrumentState.fwhmMultipliers.right
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
        A = fSquared * multiplicity * dSpacing**4
        A = np.append(A, 0)

        self.goodPeaks = [peak for i, peak in enumerate(crystalInfo.peaks) if A[i] >= 0]

        self.purgeDuplicates = self.getProperty("PurgeDuplicates").value

        self.allGroupIDs = ingredients.pixelGroup.groupIDs

    def PyExec(self) -> None:
        ingredients = PeakIngredients.parse_raw(self.getProperty("Ingredients").value)
        self.chopIngredients(ingredients)

        allFocusGroupsPeaks = []
        for groupID in self.allGroupIDs:
            # select only good peaks within the d-spacing range
            crystalPeakList = [
                peak for peak in self.goodPeaks if self.dMin[groupID] <= peak.dSpacing <= self.dMax[groupID]
            ]
            dList = [peak.dSpacing for peak in crystalPeakList]

            singleFocusGroupPeaks = []
            for crystalPeak in crystalPeakList:
                d = crystalPeak.dSpacing
                # beta terms
                beta_T = self.beta_0 + self.beta_1 / d**4  # GSAS-I beta
                beta_d = (
                    self.BETA_D_COEFFICIENT * self.L * np.sin(self.tTheta[groupID] / 2) * beta_T
                )  # converted to d-space

                fwhm = self.FWHM * self.delDoD[groupID] * d
                widthLeft = fwhm * self.FWHMMultiplierLeft
                widthRight = fwhm * self.FWHMMultiplierRight + self.peakTailCoefficient / beta_d

                if self.purgeDuplicates:
                    d = round(d, 5)

                singleFocusGroupPeaks.append(
                    DetectorPeak(
                        position=LimitedValue(value=d, minimum=d - widthLeft, maximum=d + widthRight), peak=crystalPeak
                    )
                )

            if self.purgeDuplicates:
                singleFocusGroupPeaks = list({peak.position.value: peak for peak in singleFocusGroupPeaks}.values())

            maxFwhm = self.FWHM * max(dList, default=0.0) * self.delDoD[groupID]

            singleFocusGroupPeakList = GroupPeakList(peaks=singleFocusGroupPeaks, groupID=groupID, maxfwhm=maxFwhm)
            self.log().notice(f"Focus group {groupID} : {len(dList)} peaks out")
            allFocusGroupsPeaks.append(singleFocusGroupPeakList)

        self.setProperty("DetectorPeaks", list_to_raw(allFocusGroupsPeaks))
        return allFocusGroupsPeaks


AlgorithmFactory.subscribe(DetectorPeakPredictor)
# Puts function in simpleapi globals
algo = DetectorPeakPredictor()
algo.initialize()
_create_algorithm_function(DetectorPeakPredictor.__name__, 1, algo)
