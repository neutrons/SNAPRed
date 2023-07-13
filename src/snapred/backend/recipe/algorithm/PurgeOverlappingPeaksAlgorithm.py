import json

from mantid.api import AlgorithmFactory, PythonAlgorithm
from mantid.kernel import Direction

from snapred.backend.dao.DetectorPeak import DetectorPeak
from snapred.backend.recipe.algorithm.DetectorPeakPredictor import DetectorPeakPredictor

name = "PurgeOverlappingPeaksAlgorithm"


class PurgeOverlappingPeaksAlgorithm(PythonAlgorithm):
    def PyInit(self):
        # declare properties
        self.declareProperty("InstrumentState", defaultValue="", direction=Direction.Input)
        self.declareProperty("CrystalInfo", defaultValue="", direction=Direction.Input)
        self.declareProperty("OutputPeakMap", defaultValue="", direction=Direction.Output)
        self.setRethrows(True)

    def PyExec(self):
        # predict detector peaks for all focus groups
        peakPredictorAlgo = DetectorPeakPredictor()
        peakPredictorAlgo.initialize()
        peakPredictorAlgo.setProperty("InstrumentState", self.getProperty("InstrumentState").value)
        peakPredictorAlgo.setProperty("CrystalInfo", self.getProperty("CrystalInfo").value)
        peakPredictorAlgo.execute()
        predictedPeaks_json = json.loads(peakPredictorAlgo.getProperty("DetectorPeaks").value)

        # build lists of non-overlapping peaks for each focus group. Combine them into the total list.
        outputPeaks = []
        for focusGroupPeaks_json in predictedPeaks_json:
            # build a list of DetectorPeak objects for this focus group
            peakList = []
            for peak_json in focusGroupPeaks_json:
                peakList.append(DetectorPeak.parse_raw(peak_json))

            # do the overlap rejection logic
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
                        outputPeakList.append(peakList[i].json())
            # and add last peak:
            if nPks > 0 and keep[-1]:
                outputPeakList.append(peakList[-1].json())

            self.log().notice(f" {nPks} peaks in and {len(outputPeakList)} peaks out")
            outputPeaks.append(outputPeakList)

        self.setProperty("OutputPeakMap", json.dumps(outputPeaks))

        return outputPeaks


AlgorithmFactory.subscribe(PurgeOverlappingPeaksAlgorithm)
