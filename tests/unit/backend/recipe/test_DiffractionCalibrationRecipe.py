import json
import unittest
from typing import List
from unittest import mock

import pytest
from mantid.api import PythonAlgorithm
from mantid.kernel import Direction
from snapred.backend.dao.DetectorPeak import DetectorPeak
from snapred.backend.dao.DiffractionCalibrationIngredients import DiffractionCalibrationIngredients as TheseIngredients
from snapred.backend.dao.GroupPeakList import GroupPeakList
from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.dao.state.FocusGroup import FocusGroup
from snapred.backend.dao.state.InstrumentState import InstrumentState
from snapred.backend.recipe.DiffractionCalibrationRecipe import DiffractionCalibrationRecipe as ThisRecipe
from snapred.meta.Config import Resource

TheAlgorithmManager: str = "snapred.backend.recipe.DiffractionCalibrationRecipe.AlgorithmManager"


class TestDiffractionCalibtationRecipe(unittest.TestCase):
    def setUp(self):
        self.fakeDBin = -abs(0.001)
        self.fakeRunNumber = "555"
        fakeRunConfig = RunConfig(runNumber=str(self.fakeRunNumber))

        fakeInstrumentState = InstrumentState.parse_raw(Resource.read("/inputs/calibration/sampleInstrumentState.json"))
        fakeInstrumentState.particleBounds.tof.minimum = 10
        fakeInstrumentState.particleBounds.tof.maximum = 1000

        fakeFocusGroup = FocusGroup.parse_raw(Resource.read("/inputs/calibration/sampleFocusGroup.json"))
        ntest = fakeFocusGroup.nHst
        fakeFocusGroup.dBin = [-abs(self.fakeDBin)] * ntest
        fakeFocusGroup.dMax = [float(x) for x in range(100 * ntest, 101 * ntest)]
        fakeFocusGroup.dMin = [float(x) for x in range(ntest)]
        fakeFocusGroup.FWHM = [5 for x in range(ntest)]
        fakeFocusGroup.definition = Resource.getPath("inputs/calibration/fakeSNAPFocGroup_Column.xml")

        peakList = [
            DetectorPeak.parse_obj({"position": {"value": 2, "minimum": 2, "maximum": 3}}),
            DetectorPeak.parse_obj({"position": {"value": 5, "minimum": 4, "maximum": 6}}),
        ]

        self.fakeIngredients = TheseIngredients(
            runConfig=fakeRunConfig,
            focusGroup=fakeFocusGroup,
            instrumentState=fakeInstrumentState,
            groupedPeakLists=[GroupPeakList(groupID=3, peaks=peakList)],
            threshold=0.5,
            calPath=Resource.getPath("outputs/calibration/"),
        )
        self.recipe = ThisRecipe()

    # TODO: once recipe implemented, this should do something
    def test_chop_ingredients(self):
        assert not self.recipe.chopIngredients(self.fakeIngredients)

    @mock.patch(TheAlgorithmManager)
    def test_execute_successful(self, mock_AlgorithmManager):
        # a scoped dummy algorithm to test the recipe's behavior
        class DummyCalibrationAlgorithm(PythonAlgorithm):
            def PyInit(self):
                # declare properties of offset algo
                self.declareProperty("Ingredients", defaultValue="", direction=Direction.Input)  # noqa: F821
                self.declareProperty("CalibrationTable", defaultValue="", direction=Direction.Output)
                self.declareProperty("data", defaultValue="", direction=Direction.Output)
                # declare properties of calibration algo
                self.declareProperty("InputWorkspace", defaultValue="", direction=Direction.Input)
                self.declareProperty("OutputWorkspace", defaultValue="", direction=Direction.Output)
                self.declareProperty("PreviousCalibrationTable", defaultValue="", direction=Direction.Input)
                self.declareProperty("FinalCalibrationTable", defaultValue="", direction=Direction.Output)

            def PyExec(self):
                self.medianOffset: float = 4.0
                self.reexecute()
                self.setProperty("PreviousCalibrationTable", self.getProperty("CalibrationTable").value)
                self.setProperty("OutputWorkspace", self.getProperty("InputWorkspace").value)
                self.setProperty("FinalCalibrationTable", self.getProperty("PreviousCalibrationTable").value)

            def reexecute(self):
                self.medianOffset *= 0.5
                data = {"medianOffset": self.medianOffset}
                self.setProperty("data", json.dumps(data))
                self.setProperty("CalibrationTable", "fake calibration table")

        mockAlgo = DummyCalibrationAlgorithm()
        mockAlgo.initialize()
        mock_AlgorithmManager.create.return_value = mockAlgo

        result = self.recipe.executeRecipe(self.fakeIngredients)
        assert result["result"]
        assert len(result["steps"]) == 3
        assert [x["medianOffset"] for x in result["steps"]] == [2.0, 1.0, 0.5]
        assert result["calibrationTable"] == mockAlgo.getProperty("FinalCalibrationTable").value

    @mock.patch(TheAlgorithmManager)
    def test_execute_unsuccessful(self, mock_AlgorithmManager):
        mock_algo = mock.Mock()
        mock_algo.execute.side_effect = RuntimeError("passed")
        mock_AlgorithmManager.create.return_value = mock_algo

        try:
            self.recipe.executeRecipe(self.fakeIngredients)
        except Exception as e:  # noqa: E722 BLE001
            assert str(e) == "passed"  # noqa: PT017
            mock_algo.execute.assert_called_once()
        else:
            # fail if execute did not raise an exception
            pytest.fail("Test should have raised RuntimeError, but no error raised")

        mock_algo.setProperty.assert_called_once_with("Ingredients", self.fakeIngredients.json())
        mock_AlgorithmManager.create.assert_called_once_with("CalculateOffsetDIFC")

    @mock.patch(TheAlgorithmManager)
    def test_execute_unsuccessful_later_calls(self, mock_AlgorithmManager):
        mock.Mock()

        # a scoped dummy algorithm to test all three try/except blocks
        class DummyFailingAlgo(PythonAlgorithm):
            def PyInit(self):
                self.declareProperty("Ingredients", defaultValue="", direction=Direction.Input)
                self.declareProperty("fails", defaultValue=0, direction=Direction.Input)
                self.declareProperty("times", defaultValue=0, direction=Direction.InOut)
                self.declareProperty("data", defaultValue="", direction=Direction.Output)

            def PyExec(self):
                self.fails = self.getProperty("fails").value
                self.reexecute()

            def reexecute(self):
                times = self.getProperty("times").value
                self.setProperty("data", json.dumps({"medianOffset": 2 ** (-times)}))
                times += 1
                if times >= self.fails:
                    raise RuntimeError(f"passed {times}")
                self.setProperty("times", times)

        mockAlgo = DummyFailingAlgo()
        mockAlgo.initialize()

        for i in range(1, 4):
            mockAlgo.setProperty("fails", i)

            # mock_algo.execute.side_effect = RuntimeError("passed")
            mock_AlgorithmManager.create.return_value = mockAlgo

            try:
                self.recipe.executeRecipe(self.fakeIngredients)
            except Exception as e:  # noqa: E722 BLE001
                assert str(e) == f"passed {i}"  # noqa: PT017
            else:
                # fail if execute did not raise an exception
                pytest.fail("Test should have raised RuntimeError, but no error raised")


# this at teardown removes the loggers, eliminating logger error printouts
# see https://github.com/pytest-dev/pytest/issues/5502#issuecomment-647157873
@pytest.fixture(autouse=True)
def clear_loggers():  # noqa: PT004
    """Remove handlers from all loggers"""
    import logging

    loggers = [logging.getLogger()] + list(logging.Logger.manager.loggerDict.values())
    for logger in loggers:
        handlers = getattr(logger, "handlers", [])
        for handler in handlers:
            logger.removeHandler(handler)
