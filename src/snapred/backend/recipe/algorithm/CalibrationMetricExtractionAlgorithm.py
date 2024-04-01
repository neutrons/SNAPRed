import json
from typing import Dict, List

import numpy as np
from mantid.api import AlgorithmFactory, PythonAlgorithm, WorkspaceGroupProperty
from mantid.kernel import Direction
from pydantic import parse_raw_as

from snapred.backend.dao.calibration.CalibrationMetric import CalibrationMetric
from snapred.backend.dao.state import PixelGroup
from snapred.backend.recipe.algorithm.FitMultiplePeaksAlgorithm import FitOutputEnum
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper


class CalibrationMetricExtractionAlgorithm(PythonAlgorithm):
    """
    This algorithm is used to extract calibration metrics from the output of the peak fitting algorithm FitMultiplePeaks
    It calculates the Sigma and Strain averages/standard deviations, and then packages them with the two theta average.
    This is done per group, strain and sigma collected over spectra within said group.
    """

    def category(self):
        return "SNAPRed Calibration"

    def PyInit(self):
        # declare properties
        self.declareProperty(WorkspaceGroupProperty("InputWorkspace", defaultValue="", direction=Direction.Input))
        self.declareProperty("PixelGroup", defaultValue="", direction=Direction.Input)
        self.declareProperty("OutputMetrics", defaultValue="", direction=Direction.Output)
        self.mantidSnapper = MantidSnapper(self, __name__)
        self.setRethrows(True)

    def validateMetric(self, metrics):
        errorReport = ""
        for groupIndex, metric in enumerate(metrics):
            if np.isnan(np.array(list(metric.values()))).any():
                errorReport += f"\nIll-fitted peaks detected in group {groupIndex + 1}."

        if errorReport != "":
            raise RuntimeError((errorReport + "\nPlease tweak your parameters and try again.\n\n"))

    def validateInputs(self) -> Dict[str, str]:
        errors = {}
        inputWorkspace = self.getProperty("InputWorkspace").value
        if inputWorkspace.getNumberOfEntries() != len(FitOutputEnum):
            errors[
                "InputWorkspace"
            ] = f"Number of groups in InputWorkspace must be divisible by {len(FitOutputEnum)}.  \
                    Did you use FitMultiplePeaksAlgorithm to get InputWorkspace?"
        return errors

    def PyExec(self):
        inputWorkspace = self.getProperty("InputWorkspace").value
        # inputWorkspace = self.mantidSnapper.mtd[inputWorkspace]

        pixelGroup = PixelGroup.parse_raw(self.getPropertyValue("PixelGroup"))
        pixelGroupingParameters = list(pixelGroup.pixelGroupingParameters.values())
        # collect all params and peak positions
        peakPos = inputWorkspace.getItem(FitOutputEnum.PeakPosition.value)
        parameters = inputWorkspace.getItem(FitOutputEnum.Parameters.value)
        workspace = inputWorkspace.getItem(FitOutputEnum.Workspace.value)  # noqa: F841
        parameterError = inputWorkspace.getItem(FitOutputEnum.ParameterError.value)  # noqa: F841

        peakMetrics = []
        for index in range(peakPos.getNumberHistograms()):
            params = [parameters.row(i) for i, wsindex in enumerate(parameters.column("wsindex")) if wsindex == index]

            sigmaAverage = 0
            sigmaStandardDeviation = 0
            strainAverage = 0
            strainStandardDeviation = 0
            twoThetaAverage = 0

            strains = []
            sigmas = []
            for rowIndex, row in enumerate(params):
                sig = row["Sigma"]
                pos = peakPos.readY(index)[rowIndex]
                d_ref = peakPos.readX(index)[rowIndex]
                if pos < 0:
                    continue
                strains.append((d_ref - pos) / sig)
                sigmas.append(sig / pos)

            strains = np.array(strains)

            sigmas = np.array(sigmas)

            sigmaAverage = np.average(sigmas)
            strainAverage = np.average(strains)

            # Convert to degrees from radians (?)
            twoThetaAverage = pixelGroupingParameters[index].twoTheta * (180.0 / np.pi)

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

        self.validateMetric(peakMetrics)

        self.setProperty("OutputMetrics", json.dumps(peakMetrics))


# Register algorithm with Mantid
AlgorithmFactory.subscribe(CalibrationMetricExtractionAlgorithm)
