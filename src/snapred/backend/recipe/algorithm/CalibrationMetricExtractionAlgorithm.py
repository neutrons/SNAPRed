import json

import numpy as np
from mantid.api import AlgorithmFactory, PythonAlgorithm
from mantid.kernel import Direction

from snapred.backend.dao.calibration.CalibrationMetric import CalibrationMetric
from snapred.backend.recipe.algorithm.FitMultiplePeaksAlgorithm import FitOutputEnum
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper

name = "CalibrationMetricExtractionAlgorithm"


class CalibrationMetricExtractionAlgorithm(PythonAlgorithm):
    """
    This algorithm is used to extract calibration metrics from the output of the peak fitting algorithm FitMultiplePeaks
    """

    def PyInit(self):
        # declare properties
        self.declareProperty("InputWorkspace", defaultValue="", direction=Direction.Input)
        self.declareProperty("OutputMetrics", defaultValue="", direction=Direction.Output)
        self.mantidSnapper = MantidSnapper(self, name)

    def PyExec(self):
        inputWorkspace = self.getProperty("InputWorkspace").value
        inputWorkspace = self.mantidSnapper.mtd[inputWorkspace]
        # collect all params and peak positions
        fitDataList = []
        for increment in range(0, inputWorkspace.getNumberOfEntries(), len(FitOutputEnum)):
            peakPosition = inputWorkspace.getItem(increment + FitOutputEnum.PeakPosition.value)
            parameters = inputWorkspace.getItem(increment + FitOutputEnum.Parameters.value)
            workspace = inputWorkspace.getItem(increment + FitOutputEnum.Workspace.value)
            parameterError = inputWorkspace.getItem(increment + FitOutputEnum.ParameterError.value)
            fitDataTuple = (peakPosition, parameters, workspace, parameterError)
            fitDataList.append(fitDataTuple)

        peakMetrics = []
        for peakPos, params, workspace, _ in fitDataList:
            sigmaAverage = 0
            sigmaStandardDeviation = 0
            strainAverage = 0
            strainStandardDeviation = 0
            twoThetaAverage = 0
            spectrumInfo = workspace.spectrumInfo()
            self.log().notice("------------------------------------------------------------------------------")
            strains = []
            sigmas = []
            for rowIndex in range(params.rowCount()):
                row = params.row(rowIndex)
                sig = row["Sigma"]
                pos = peakPos.readY(0)[rowIndex]
                d_ref = peakPos.readX(0)[rowIndex]
                if pos < 0:
                    continue
                strains.append((d_ref - pos) / sig)
                sigmas.append(sig / pos)
                self.log().notice(f"sig: {sig}, pos: {pos}, d_ref: {d_ref}")

            for spectrumIndex in range(workspace.getNumberHistograms()):
                twoTheta = spectrumInfo.twoTheta(spectrumIndex)
                twoThetaAverage += twoTheta

            strains = np.array(strains)
            sigmas = np.array(sigmas)

            sigmaAverage = np.average(sigmas)
            strainAverage = np.average(strains)
            twoThetaAverage = twoThetaAverage / workspace.getNumberHistograms()

            sigmaStandardDeviation = np.std(sigmas)
            strainStandardDeviation = np.std(strains)
            # ( sigmaAverage, sigmaStandardDeviation, strainAverage, strainStandardDeviation, twoThetaAverage)
            peakMetrics.append(
                CalibrationMetric(
                    sigmaAverage=sigmaAverage,
                    sigmaStandardDeviation=sigmaStandardDeviation,
                    strainAverage=strainAverage,
                    strainStandardDeviation=strainStandardDeviation,
                    twoThetaAverage=twoThetaAverage,
                ).dict()
            )

        self.setProperty("OutputMetrics", json.dumps(peakMetrics))


# Register algorithm with Mantid
AlgorithmFactory.subscribe(CalibrationMetricExtractionAlgorithm)
