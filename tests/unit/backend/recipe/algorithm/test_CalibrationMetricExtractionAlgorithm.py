import json
import unittest
from typing import List
from unittest.mock import MagicMock

import numpy as np
import pydantic
import pytest
from mantid.api import WorkspaceGroup
from mantid.simpleapi import CreateSingleValuedWorkspace, CreateWorkspace, GenerateTableWorkspaceFromListOfDict, mtd
from snapred.backend.dao.calibration.CalibrationMetric import CalibrationMetric
from snapred.backend.dao.state import PixelGroup, PixelGroupingParameters
from snapred.backend.recipe.algorithm.CalibrationMetricExtractionAlgorithm import (
    CalibrationMetricExtractionAlgorithm as Algo,
)
from snapred.meta.Config import Resource
from snapred.meta.mantid.FitPeaksOutput import FIT_PEAK_DIAG_SUFFIX, FitOutputEnum


def _removeWhitespace(string):
    return "".join(string.split())


class TestCalibrationMetricExtractionAlgorithm(unittest.TestCase):
    def test_pyexec(self):  # noqa: ARG002
        # Mock input data
        fakeInputWorkspace = "mock_input_workspace"
        vals = np.array([[1.0], [2.0], [3.0]])
        suffix = FIT_PEAK_DIAG_SUFFIX.copy()

        fakeInputWorkspaceData = {
            suffix[FitOutputEnum.PeakPosition]: MagicMock(
                readY=MagicMock(return_value=vals), readX=MagicMock(return_value=vals)
            ),
            suffix[FitOutputEnum.Parameters]: MagicMock(
                rowCount=MagicMock(return_value=3),
                row=MagicMock(side_effect=np.array([{"Sigma": 0.1}, {"Sigma": 0.2}, {"Sigma": 0.3}])),
            ),
            suffix[FitOutputEnum.Workspace]: np.array(["ws1", "ws2", "ws3"]),
            suffix[FitOutputEnum.ParameterError]: np.array([{}, {}, {}]),
        }
        fitPeaksDiagnosis = WorkspaceGroup()
        mtd.addOrReplace(fakeInputWorkspace, fitPeaksDiagnosis)
        # the peak positions workspace
        CreateWorkspace(
            OutputWorkspace=suffix[FitOutputEnum.PeakPosition],
            DataX=vals,
            DataY=vals,
        )
        fitPeaksDiagnosis.add(suffix[FitOutputEnum.PeakPosition])
        # the fit parameters workspace
        GenerateTableWorkspaceFromListOfDict(
            ListOfDict=json.dumps([{"wsindex": x, "Sigma": (x + 1.0) / 10.0} for x in [0, 1, 2]]),
            OutputWorkspace=suffix[FitOutputEnum.Parameters],
        )
        fitPeaksDiagnosis.add(suffix[FitOutputEnum.Parameters])
        # the fitted peaks workspace
        CreateSingleValuedWorkspace(Outputworkspace=suffix[FitOutputEnum.Workspace])
        fitPeaksDiagnosis.add(suffix[FitOutputEnum.Workspace])
        # the parameter error workspace
        CreateSingleValuedWorkspace(OutputWorkspace=suffix[FitOutputEnum.ParameterError])
        fitPeaksDiagnosis.add(suffix[FitOutputEnum.ParameterError])

        fakePixelGroupingParameterss = [
            PixelGroupingParameters(
                groupID=0,
                isMasked=False,
                L2=10.0,
                twoTheta=30.0 * (np.pi / 180.0),
                azimuth=0.0,
                dResolution={"minimum": 0.1, "maximum": 0.2},
                dRelativeResolution=0.1,
            ),
            PixelGroupingParameters(
                groupID=1,
                isMasked=False,
                L2=10.0,
                twoTheta=40.0 * (np.pi / 180.0),
                azimuth=0.0,
                dResolution={"minimum": 0.1, "maximum": 0.2},
                dRelativeResolution=0.1,
            ),
            PixelGroupingParameters(
                groupID=2,
                isMasked=False,
                L2=10.0,
                azimuth=0.0,
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
        # mock out the mtd return
        algorithm.mantidSnapper = MagicMock()
        algorithm.mantidSnapper.mtd = {
            fakeInputWorkspace: MagicMock(
                getNumberOfEntries=MagicMock(return_value=4),
                getItem=MagicMock(side_effect=fakeInputWorkspaceData.values()),
            )
        }
        # Call the PyExec method to test
        algorithm.execute()

        # Get the output metrics property and parse it as a list of dictionaries
        output_metrics = pydantic.TypeAdapter(List[CalibrationMetric]).validate_json(
            algorithm.getProperty("OutputMetrics").value
        )

        # Test data is currently not the greatest
        expected = pydantic.TypeAdapter(List[CalibrationMetric]).validate_json(
            Resource.read("outputs/calibration/metrics/expected.json")
        )

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
