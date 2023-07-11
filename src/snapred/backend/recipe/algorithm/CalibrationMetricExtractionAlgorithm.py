import json
from math import sqrt

from mantid.api import AlgorithmFactory, PythonAlgorithm
from mantid.kernel import Direction
from pydantic import parse_raw_as

from snapred.backend.dao.CrystallographicInfo import CrystallographicInfo
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper

name = "CalibrationMetricExtractionAlgorithm"


class CalibrationMetricExtractionAlgorithm(PythonAlgorithm):
    """
    This algorithm is used to extract calibration metrics from the output of the peak fitting algorithm FitMultiplePeaks
    """

    def PyInit(self):
        # declare properties
        self.declareProperty("InputWorkspace", defaultValue="", direction=Direction.Input)
        self.declareProperty("CrystallographicInfo", defaultValue="", direction=Direction.Input)
        self.declareProperty("OutputMetrics", defaultValue="", direction=Direction.Output)
        self.mantidSnapper = MantidSnapper(self, name)

    def PyExec(self):
        inputWorkspace = self.getProperty("InputWorkspace").value
        crystallographicInfo = parse_raw_as(CrystallographicInfo, self.getProperty("CrystallographicInfo").value)
        fittedPeakPos = "_fitted_peakpositions"
        fittedParams = "_fitted_params"
        fittedParamsErr = "_fitted_params_err"
        inputWorkspace = self.mantidSnapper.mtd[inputWorkspace]
        # collect all params and peak positions
        paramNames = []
        peakPositionNames = []
        for workspaceIndex in range(inputWorkspace.getNumberOfEntries()):
            ws = inputWorkspace.getItem(workspaceIndex)
            wsName = ws.getName()
            if fittedPeakPos in wsName:
                peakPositionNames.append(wsName)
            elif fittedParams in wsName and fittedParamsErr not in wsName:
                paramNames.append(wsName)

        peakMetrics = []
        for paramName, peakPositionName, workspaceIndex in zip(paramNames, peakPositionNames, range(len(paramNames))):
            peakPos = self.mantidSnapper.mtd[peakPositionName]
            params = self.mantidSnapper.mtd[paramName]
            sigAvg = 0
            sigStd = 0
            posAvg = 0
            posStd = 0
            twoThetaAvg = 0
            for rowIndex in range(params.rowCount()):
                row = params.row(rowIndex)
                sig = row["Sigma"]
                twoTheta = row["PeakCentre"]
                pos = peakPos.readY(0)[rowIndex]
                d_ref = crystallographicInfo.peaks[rowIndex].dSpacing
                sigAvg += sig / pos
                posAvg += (d_ref - pos) / sig
                twoThetaAvg += twoTheta
            numPeaks = params.rowCount()
            sigAvg = sigAvg / numPeaks
            posAvg = posAvg / numPeaks
            twoThetaAvg = twoThetaAvg / numPeaks

            # calc standard deviation of sig and pos
            for rowIndex in range(params.rowCount()):
                sig = row["Sigma"]
                pos = peakPos.readY(0)[rowIndex]
                sigStd += (sig - sigAvg) ** 2
                posStd += (pos - posAvg) ** 2
            sigStd = sqrt(sigStd / numPeaks)
            posStd = sqrt(posStd / numPeaks)
            # (sigAvg, sigStd, posAvg, posStd, twoThetaAvg)
            peakMetrics.append(
                {"sigAvg": sigAvg, "sigStd": sigStd, "posAvg": posAvg, "posStd": posStd, "twoThetaAvg": twoThetaAvg}
            )

        self.setProperty("OutputMetrics", json.dumps(peakMetrics))


# Register algorithm with Mantid
AlgorithmFactory.subscribe(CalibrationMetricExtractionAlgorithm)
