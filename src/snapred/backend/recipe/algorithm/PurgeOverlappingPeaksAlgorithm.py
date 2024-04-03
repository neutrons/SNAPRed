import json
from typing import Dict, List

import numpy as np
from mantid.api import AlgorithmFactory, PythonAlgorithm
from mantid.kernel import Direction
from pydantic import parse_raw_as

from snapred.backend.dao import CrystallographicInfo
from snapred.backend.dao.GroupPeakList import GroupPeakList
from snapred.backend.dao.ingredients import PeakIngredients
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.algorithm.DetectorPeakPredictor import DetectorPeakPredictor
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper
from snapred.meta.Config import Config
from snapred.meta.redantic import list_to_raw

logger = snapredLogger.getLogger(__name__)


class PurgeOverlappingPeaksAlgorithm(PythonAlgorithm):
    D_MIN = Config["constants.CrystallographicInfo.dMin"]
    D_MAX = Config["constants.CrystallographicInfo.dMax"]

    def category(self):
        return "SNAPRed Data Processing"

    def PyInit(self):
        # declare properties
        self.declareProperty(
            "Ingredients",
            defaultValue="",
            direction=Direction.Input,
            doc="The CrystalInfo and Intensity Threshold ingredients",
        )
        self.declareProperty(
            "DetectorPeaks",
            defaultValue="",
            direction=Direction.Input,
            doc="Input list of peaks",
        )
        self.declareProperty(
            "OutputPeakMap",
            defaultValue="",
            direction=Direction.Output,
            doc="The resulting, non-overlapping list of peaks",
        )
        self.declareProperty("dMin", defaultValue=self.D_MIN, direction=Direction.Input)
        self.declareProperty("dMax", defaultValue=self.D_MAX, direction=Direction.Input)
        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, __name__)

    def calcA(self, peaks):
        peaks.sort(key=lambda x: x.dSpacing)
        fSquared = np.array([peak.fSquared for peak in peaks])
        multiplicity = np.array([peak.multiplicity for peak in peaks])
        dSpacing = np.array([peak.dSpacing for peak in peaks])
        A = fSquared * multiplicity * dSpacing**4
        A = np.append(A, 0)
        return A

    def chopIngredients(self, ingredients):  # noqa ARG002
        crystalInfo = ingredients.crystalInfo
        A = self.calcA(crystalInfo.peaks)
        self.thresholdA = np.max(A) * ingredients.peakIntensityThreshold
        self.dMin = float(self.getPropertyValue("dMin"))
        self.dMax = float(self.getPropertyValue("dMax"))

    def unbagGroceries(self):
        # NOTE there are no input workspaces
        pass

    def validateInputs(self) -> Dict[str, str]:
        errors = {}
        return errors

    def filterPeaksOnIntensity(self, peakLists: List[GroupPeakList]) -> List[GroupPeakList]:
        for groupPeakList in peakLists:
            startingCount = len(groupPeakList.peaks)
            detectorPeaks = groupPeakList.peaks
            detectorPeaks.sort(key=lambda x: x.peak.dSpacing)
            peaks = [detectorPeak.peak for detectorPeak in detectorPeaks]
            A = self.calcA(peaks)
            groupPeakList.peaks = [
                detectorPeak for i, detectorPeak in enumerate(detectorPeaks) if A[i] >= self.thresholdA
            ]
            self.log().notice(
                (
                    f"Purged {startingCount - len(groupPeakList.peaks)} peaks from group {groupPeakList.groupID}"
                    " because of intensity"
                )
            )
        return peakLists

    def filterPeaksOnDRange(self, peakLists: List[GroupPeakList]) -> List[GroupPeakList]:
        for groupPeakList in peakLists:
            startingCount = len(groupPeakList.peaks)
            detectorPeaks = groupPeakList.peaks
            detectorPeaks.sort(key=lambda x: x.peak.dSpacing)
            groupPeakList.peaks = [
                detectorPeak
                for detectorPeak in detectorPeaks
                if detectorPeak.peak.dSpacing >= self.dMin and detectorPeak.peak.dSpacing <= self.dMax
            ]
            self.log().notice(
                (
                    f"Purged {startingCount - len(groupPeakList.peaks)} peaks from group {groupPeakList.groupID},"
                    " because they were outside the d-spacing range"
                )
            )
        return peakLists

    def PyExec(self):
        predictedPeaks = parse_raw_as(List[GroupPeakList], self.getPropertyValue("DetectorPeaks"))
        ingredients = PeakIngredients.parse_raw(self.getProperty("Ingredients").value)
        self.chopIngredients(ingredients)

        # build lists of non-overlapping peaks for each focus group. Combine them into the total list.
        outputPeaks = []
        for groupPeakList in predictedPeaks:
            # ensure peaks are unique
            uniquePeaks = {peak.position.value: peak for peak in groupPeakList.peaks}.values()

            # do the overlap rejection logic on unique peaks
            peakList = list(uniquePeaks)
            nPks = len(peakList)
            keep = [True for i in range(nPks)]
            outputPeakList = []
            for i in range(nPks - 1):
                if keep[i]:
                    if (peakList[i].position.maximum >= peakList[i + 1].position.minimum) and (
                        peakList[i + 1].position.value != peakList[i].position.value
                    ):
                        keep[i] = False
                        keep[i + 1] = False
                    else:
                        outputPeakList.append(peakList[i])
            # and add last peak:
            if nPks > 0 and keep[-1]:
                outputPeakList.append(peakList[-1])

            self.log().notice(f" {nPks} peaks in and {len(outputPeakList)} peaks out")
            outputGroupPeakList = GroupPeakList(
                groupID=groupPeakList.groupID,
                peaks=outputPeakList,
                maxfwhm=groupPeakList.maxfwhm,
            )
            outputPeaks.append(outputGroupPeakList)
        outputPeaks = self.filterPeaksOnIntensity(outputPeaks)
        outputPeaks = self.filterPeaksOnDRange(outputPeaks)
        self.setProperty("OutputPeakMap", list_to_raw(outputPeaks))

        return outputPeaks


(AlgorithmFactory.subscribe(PurgeOverlappingPeaksAlgorithm),)
