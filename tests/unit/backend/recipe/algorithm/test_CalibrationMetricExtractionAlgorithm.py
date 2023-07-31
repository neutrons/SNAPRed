import unittest
from unittest.mock import MagicMock, patch

import numpy as np
from mantid.api import AlgorithmManager
from mantid.kernel import Direction
from pydantic import parse_raw_as
from snapred.backend.dao.calibration.CalibrationMetric import CalibrationMetric
from snapred.backend.dao.state.PixelGroupingParameters import PixelGroupingParameters
from snapred.backend.recipe.algorithm.CalibrationMetricExtractionAlgorithm import CalibrationMetricExtractionAlgorithm
from snapred.meta.Config import Resource
from snapred.meta.redantic import list_to_raw


def _removeWhitespace(string):
    return "".join(string.split())


class TestCalibrationMetricExtractionAlgorithm(unittest.TestCase):
    # patch the MantidSnapper object
    @patch(
        "snapred.backend.recipe.algorithm.CalibrationMetricExtractionAlgorithm.MantidSnapper",
        return_value=MagicMock(mtd=MagicMock(return_value={"mock_input_workspace": {}})),
    )
    def test_pyexec(self, mock_mantid_snapper):  # noqa: ARG002
        # Mock input data
        input_workspace = "mock_input_workspace"
        vals = np.array([[1.0], [2.0], [3.0]])
        input_workspace_data = {
            "PeakPosition": MagicMock(readY=MagicMock(return_value=vals), readX=MagicMock(return_value=vals)),
            "Parameters": MagicMock(
                rowCount=MagicMock(return_value=3),
                row=MagicMock(side_effect=np.array([{"Sigma": 0.1}, {"Sigma": 0.2}, {"Sigma": 0.3}])),
            ),
            "Workspace": np.array(["ws1", "ws2", "ws3"]),
            "ParameterError": np.array([{}, {}, {}]),
        }
        pixel_grouping_parameter = [
            PixelGroupingParameters(twoTheta=30, dResolution={"minimum": 0.1, "maximum": 0.2}, dRelativeResolution=0.1),
            PixelGroupingParameters(twoTheta=40, dResolution={"minimum": 0.1, "maximum": 0.2}, dRelativeResolution=0.1),
            PixelGroupingParameters(twoTheta=50, dResolution={"minimum": 0.1, "maximum": 0.2}, dRelativeResolution=0.1),
        ]
        [
            CalibrationMetric(
                sigmaAverage=0.1,
                sigmaStandardDeviation=0.0,
                strainAverage=-0.0,
                strainStandardDeviation=0.0,
                twoThetaAverage=30,
            ).dict(),
            CalibrationMetric(
                sigmaAverage=0.2,
                sigmaStandardDeviation=0.0,
                strainAverage=-0.0,
                strainStandardDeviation=0.0,
                twoThetaAverage=40,
            ).dict(),
            CalibrationMetric(
                sigmaAverage=0.3,
                sigmaStandardDeviation=0.0,
                strainAverage=-0.0,
                strainStandardDeviation=0.0,
                twoThetaAverage=50,
            ).dict(),
        ]

        # Create the algorithm instance and set properties
        algorithm = CalibrationMetricExtractionAlgorithm()
        algorithm.initialize()
        algorithm.setProperty("InputWorkspace", input_workspace)
        algorithm.setProperty("PixelGroupingParameter", list_to_raw(pixel_grouping_parameter))
        algorithm.mantidSnapper.mtd = {
            input_workspace: MagicMock(
                getNumberOfEntries=MagicMock(return_value=4),
                getItem=MagicMock(side_effect=input_workspace_data.values()),
            )
        }
        # Call the PyExec method to test
        algorithm.execute()

        # Get the output metrics property and parse it as a list of dictionaries
        output_metrics = algorithm.getProperty("OutputMetrics").value

        # Test data is currently not the greatest
        expected = Resource.read("outputs/calibration/metrics/expected.json")

        # Assert the output metrics are as expected
        assert _removeWhitespace(output_metrics) == _removeWhitespace(expected)
