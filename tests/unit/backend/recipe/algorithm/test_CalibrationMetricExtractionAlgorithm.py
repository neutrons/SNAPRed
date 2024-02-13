import unittest
from typing import List
import numpy as np

from unittest.mock import MagicMock, patch
import pytest

from mantid.api import AlgorithmManager
from mantid.kernel import Direction
from pydantic import parse_raw_as
from snapred.backend.dao.calibration.CalibrationMetric import CalibrationMetric
from snapred.backend.dao.state import PixelGroup, PixelGroupingParameters
from snapred.backend.recipe.algorithm.CalibrationMetricExtractionAlgorithm import CalibrationMetricExtractionAlgorithm
from snapred.meta.Config import Resource

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
        fakeInputWorkspace = "mock_input_workspace"
        vals = np.array([[1.0], [2.0], [3.0]])
        fakeInputWorkspaceData = {
            "PeakPosition": MagicMock(readY=MagicMock(return_value=vals), readX=MagicMock(return_value=vals)),
            "Parameters": MagicMock(
                rowCount=MagicMock(return_value=3),
                row=MagicMock(side_effect=np.array([{"Sigma": 0.1}, {"Sigma": 0.2}, {"Sigma": 0.3}])),
            ),
            "Workspace": np.array(["ws1", "ws2", "ws3"]),
            "ParameterError": np.array([{}, {}, {}]),
        }
        fakePixelGroupingParameterss = [
            PixelGroupingParameters(
                groupID=0, isMasked=False, twoTheta=30.0 * (np.pi / 180.0), dResolution={"minimum": 0.1, "maximum": 0.2}, dRelativeResolution=0.1
            ),
            PixelGroupingParameters(
                groupID=1, isMasked=False, twoTheta=40.0 * (np.pi / 180.0), dResolution={"minimum": 0.1, "maximum": 0.2}, dRelativeResolution=0.1
            ),
            PixelGroupingParameters(
                groupID=2, isMasked=False, twoTheta=50.0 * (np.pi / 180.0), dResolution={"minimum": 0.1, "maximum": 0.2}, dRelativeResolution=0.1
            ),
        ]
        fakePixelGroup = PixelGroup(
            pixelGroupingParameters=fakePixelGroupingParameterss,
            timeOfFlight={"minimum": 1.0, "maximum": 10.0, "binWidth": 1, "binningMode": 1},
        )

        # Create the algorithm instance and set properties
        algorithm = CalibrationMetricExtractionAlgorithm()
        algorithm.initialize()
        algorithm.setProperty("InputWorkspace", fakeInputWorkspace)
        algorithm.setProperty("PixelGroup", fakePixelGroup.json())
        algorithm.mantidSnapper.mtd = {
            fakeInputWorkspace: MagicMock(
                getNumberOfEntries=MagicMock(return_value=4),
                getItem=MagicMock(side_effect=fakeInputWorkspaceData.values()),
            )
        }
        # Call the PyExec method to test
        algorithm.execute()

        # Get the output metrics property and parse it as a list of dictionaries
        output_metrics = parse_raw_as(List[CalibrationMetric], algorithm.getProperty("OutputMetrics").value)

        # Test data is currently not the greatest
        expected = parse_raw_as(List[CalibrationMetric], Resource.read("outputs/calibration/metrics/expected.json"))
       
        # Assert the output metrics are as expected
        for metric in output_metrics[0].dict():
            assert pytest.approx(expected[0].dict()[metric], 1.0e-6) == output_metrics[0].dict()[metric]
