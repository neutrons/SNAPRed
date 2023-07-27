import json
from typing import List

import numpy as np
from mantid.api import AlgorithmFactory, PythonAlgorithm
from mantid.kernel import Direction
from pydantic import parse_raw_as

from snapred.backend.dao.calibration.CalibrationMetric import CalibrationMetric
from snapred.backend.dao.state.PixelGroupingParameters import PixelGroupingParameters
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
        self.declareProperty("PixelGroupingParameter", defaultValue="", direction=Direction.Input)
        self.declareProperty("OutputMetrics", defaultValue="", direction=Direction.Output)
        self.mantidSnapper = MantidSnapper(self, name)
        self.setRethrows(True)

    def PyExec(self):
        inputWorkspace = self.getProperty("InputWorkspace").value
        inputWorkspace = self.mantidSnapper.mtd[inputWorkspace]
        pixelGroupingParameters = parse_raw_as(
            List[PixelGroupingParameters], self.getProperty("PixelGroupingParameter").value
        )
        # collect all params and peak positions
        fitDataList = []
        for increment in range(0, inputWorkspace.getNumberOfEntries(), len(FitOutputEnum)):
            peakPosition = inputWorkspace.getItem(increment + FitOutputEnum.PeakPosition.value)
            parameters = inputWorkspace.getItem(increment + FitOutputEnum.Parameters.value)
            workspace = inputWorkspace.getItem(increment + FitOutputEnum.Workspace.value)
            parameterError = inputWorkspace.getItem(increment + FitOutputEnum.ParameterError.value)
            fitDataTuple = (peakPosition, parameters, workspace, parameterError, int(increment / len(FitOutputEnum)))
            fitDataList.append(fitDataTuple)

        peakMetrics = []
        for peakPos, params, workspace, _, index in fitDataList:
            sigmaAverage = 0
            sigmaStandardDeviation = 0
            strainAverage = 0
            strainStandardDeviation = 0
            twoThetaAverage = 0

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

            strains = np.array(strains)

            sigmas = np.array(sigmas)

            sigmaAverage = np.average(sigmas)
            strainAverage = np.average(strains)
            twoThetaAverage = pixelGroupingParameters[index].twoTheta

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
