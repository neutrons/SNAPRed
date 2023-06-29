

from typing import List

from mantid.api import AlgorithmFactory, PythonAlgorithm
from mantid.kernel import Direction
from pydantic import parse_raw_as

from snapred.backend.dao.CrystallographicInfo import CrystallographicInfo
from snapred.backend.dao.state.PixelGroupingInstrumentParameters import PixelGroupingInstrumentParameters
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper

name = "CalibrationMetricExtractionAlgorithm"


class CalibrationMetricExtractionAlgorithm(PythonAlgorithm):
    def PyInit(self):
        # declare properties
        self.declareProperty("InputGroupWorkspace", defaultValue="", direction=Direction.Input)
        self.declareProperty("CrystallographicInfo", defaultValue="", direction=Direction.Input)
        self.declareProperty("PixelGroupingInstrumentParameters", defaultValue="", direction=Direction.Input)
        self.declareProperty("OutputMetrics", defaultValue="", direction=Direction.Output)
        self.mantidSnapper = MantidSnapper(self, name)

    def PyExec(self):
        inputGroupWorkspace = self.getProperty("InputGroupWorkspace").value
        crystallographicInfo = parse_raw_as(CrystallographicInfo, self.getProperty("CrystallographicInfo").value)
        pixelGroupingInstrumentParameters = parse_raw_as(List[PixelGroupingInstrumentParameters], self.getProperty("PixelGroupingInstrumentParameters").value)
        fittedPeakPos = f"{wsName}_fitted_peakpositions_{index}"
        fittedParams = f"{wsName}_fitted_params_{index}"
        fittedWS = f"{wsName}_fitted_{index}"
        fittedParamsErr = f"{wsName}_fitted_params_err_{index}"
        self.mantidSnapper.mtd[inputGroupWorkspace]
        strippedWorkspaces = []
        for workspaceIndex in range(len(pixelGroupingInstrumentParameters)):
            group = inputGroupWorkspace.getItem(workspaceIndex)
            peakMetrics = []
            # for every subgroup
            #     sigAvg = 0
            #     sigStd = 0
            #     posAvg = 0
            #     posStd = 0
            #     twoThetaAvg = 0
            #     peakPos = group->subgroup->peakPos
            #     peaks = group->subgroup->params
            #     for i, peak in enumerate(peaks)
            #         # calc sig/pos and average over all peaks, with a standard deviation estimate
            #         # calc (d_ref-pos)/sig and average over all peaks where d_ref is a known peak position of the calbrant sample, with a standard deviation estimate
            #         # extract position, sig, height
            #         d_ref = crystalInfo.peaks[i].dSpacing
            #         position = peakPos.readY(i)[0]
            #         sig = peak["Sigma"]
            #         height = peak["Height"]
            #         sigAvg += sig/position
            #         posAvg += (d_ref - position)/sig
            #     sigAvg = sigAvg/numPeaks
            #     posAvg = posAvg/numPeaks
            #     twoThetaAvg = instrumentState.pixelGroupingInstrumentParameters[index].twoThetaAverage
            
            #     calc standard deviation of sig and pos
            #     for every peak
            #         sigStd += (sig - sigAvg)^2
            #         posStd += (pos - posAvg)^2
            #     sigStd = sqrt(sigStd/numPeaks)
            #     posStd = sqrt(posStd/numPeaks)
            #     peakMetrics.append((sigAvg, sigStd, posAvg, posStd, twoThetaAvg))

            # some nitty gritty details of the above
            # peakposws = mtd['peakpositions']
            # param_ws = mtd['fitted_params']
            # row = param_ws.row(0)

            # # output
            # print ('Fitted peak position: {0:.5f}'.format(peakposws.readY(0)[0]))
            # print ("Peak 0  Centre: {0:.5f}, width: {1:.5f}, height: {2:.5f}".format(row["PeakCentre"], row["Sigma"], row["Height"]))


# Register algorithm with Mantid
AlgorithmFactory.subscribe(CalibrationMetricExtractionAlgorithm)
