import json
from typing import Dict, List

from mantid.api import AlgorithmFactory, PythonAlgorithm
from mantid.kernel import Direction
from pydantic import parse_raw_as

from snapred.backend.dao.GroupPeakList import GroupPeakList
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.algorithm.DetectorPeakPredictor import DetectorPeakPredictor
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper

logger = snapredLogger.getLogger(__name__)


class PurgeOverlappingPeaksAlgorithm(PythonAlgorithm):
    def category(self):
        return "SNAPRed Data Processing"

    def PyInit(self):
        # declare properties
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
        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, __name__)

    def chopIngredients(self, ingredients):  # noqa ARG002
        # NOTE this entire algorithm is a "chopIngredients" method
        pass

    def unbagGroceries(self):
        # NOTE there are no input workspaces
        pass

    def validateInputs(self) -> Dict[str, str]:
        errors = {}
        return errors

    def PyExec(self):
        predictedPeaks = parse_raw_as(List[GroupPeakList], self.getPropertyValue("DetectorPeaks"))

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
            outputPeaks.append(outputGroupPeakList.dict())

        self.setProperty("OutputPeakMap", json.dumps(outputPeaks))

        return outputPeaks


(AlgorithmFactory.subscribe(PurgeOverlappingPeaksAlgorithm),)
