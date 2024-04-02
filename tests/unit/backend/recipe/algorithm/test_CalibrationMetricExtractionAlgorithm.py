import json
import unittest
from typing import List
from unittest.mock import MagicMock, patch

import numpy as np
import pytest
from mantid.api import AlgorithmManager, WorkspaceGroup
from mantid.kernel import Direction
from mantid.simpleapi import CreateSingleValuedWorkspace, CreateWorkspace, mtd
from pydantic import parse_raw_as
from snapred.backend.dao.calibration.CalibrationMetric import CalibrationMetric
from snapred.backend.dao.state import PixelGroup, PixelGroupingParameters
from snapred.backend.recipe.algorithm.CalibrationMetricExtractionAlgorithm import (
    CalibrationMetricExtractionAlgorithm as Algo,
)
from snapred.backend.recipe.algorithm.GenerateTableWorkspaceFromListOfDict import (
    GenerateTableWorkspaceFromListOfDict as Table,
)
from snapred.meta.Config import Resource


def _removeWhitespace(string):
    return "".join(string.split())


class TestCalibrationMetricExtractionAlgorithm(unittest.TestCase):
    def test_pyexec(self):  # noqa: ARG002
        fakeInputWorkspace = "mock_input_workspace"
        fitPeaksDiagnosis = WorkspaceGroup()
        mtd.addOrReplace(fakeInputWorkspace, fitPeaksDiagnosis)

        # Mock input data
        fakeInputWorkspace = "mock_input_workspace"
        vals = np.array([[1.0], [2.0], [3.0]])
        CreateWorkspace(
            OutputWorkspace="PeakPosition",
            DataX=vals,
            DataY=vals,
        )
        fitPeaksDiagnosis.add("PeakPosition")
        table = Table()
        table.initialize()
        table.setProperty("ListOfDict", json.dumps([{"wsindex": x, "Sigma": (x + 1.0) / 10.0} for x in [0, 1, 2]]))
        table.setProperty("OutputWorkspace", "Parameters")
        table.execute()
        fitPeaksDiagnosis.add("Parameters")
        CreateSingleValuedWorkspace(Outputworkspace="Workspace")
        fitPeaksDiagnosis.add("Workspace")
        CreateSingleValuedWorkspace(OutputWorkspace="ParameterError")
        fitPeaksDiagnosis.add("ParameterError")

        fakePixelGroupingParameterss = [
            PixelGroupingParameters(
                groupID=0,
                isMasked=False,
                twoTheta=30.0 * (np.pi / 180.0),
                dResolution={"minimum": 0.1, "maximum": 0.2},
                dRelativeResolution=0.1,
            ),
            PixelGroupingParameters(
                groupID=1,
                isMasked=False,
                twoTheta=40.0 * (np.pi / 180.0),
                dResolution={"minimum": 0.1, "maximum": 0.2},
                dRelativeResolution=0.1,
            ),
            PixelGroupingParameters(
                groupID=2,
                isMasked=False,
                twoTheta=50.0 * (np.pi / 180.0),
                dResolution={"minimum": 0.1, "maximum": 0.2},
                dRelativeResolution=0.1,
            ),
        ]
        fakePixelGroup = PixelGroup(
            pixelGroupingParameters=fakePixelGroupingParameterss,
            focusGroup={"name": "something", "definition": "path/to/wherever"},
            timeOfFlight={"minimum": 1.0, "maximum": 10.0, "binWidth": 1, "binningMode": 1},
        )

        # Create the algorithm instance and set properties
        algorithm = Algo()
        algorithm.initialize()
        algorithm.setProperty("InputWorkspace", fakeInputWorkspace)
        algorithm.setProperty("PixelGroup", fakePixelGroup.json())
        # Call the PyExec method to test
        algorithm.execute()

        # Get the output metrics property and parse it as a list of dictionaries
        output_metrics = parse_raw_as(List[CalibrationMetric], algorithm.getProperty("OutputMetrics").value)

        # Test data is currently not the greatest
        expected = parse_raw_as(List[CalibrationMetric], Resource.read("outputs/calibration/metrics/expected.json"))

        # Assert the output metrics are as expected
        for metric in output_metrics[0].dict():
            assert pytest.approx(expected[0].dict()[metric], 1.0e-6) == output_metrics[0].dict()[metric]

    def test_must_have_workspacegroup(self):
        notAWorkspaceGroup = "not_a_workspace_group"
        algo = Algo()
        with pytest.raises(RuntimeError):
            algo.setProperty("InputWorkspace", notAWorkspaceGroup)

    def test_validate(self):  # noqa: ARG002
        fakeInputWorkspace = "mock_input_workspace"
        fitPeaksDiagnosis = WorkspaceGroup()
        mtd.addOrReplace(fakeInputWorkspace, fitPeaksDiagnosis)
        for i in range(3):
            CreateSingleValuedWorkspace(OutputWorkspace=f"ws{i}")
            fitPeaksDiagnosis.add(f"ws{i}")

        fakePixelGroup = MagicMock(json=MagicMock(return_value=""))

        # Create the algorithm instance and set properties
        algorithm = Algo()
        algorithm.initialize()
        algorithm.setProperty("InputWorkspace", fakeInputWorkspace)
        algorithm.setProperty("PixelGroup", fakePixelGroup.json())
        # Call the PyExec method to test
        with pytest.raises(RuntimeError) as e:
            algorithm.execute()
        assert "InputWorkspace" in str(e.value)
