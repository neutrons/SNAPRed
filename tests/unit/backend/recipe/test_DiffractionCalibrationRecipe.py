import json
import unittest
from typing import List
from unittest import mock

import pytest
from mantid.api import PythonAlgorithm
from mantid.kernel import Direction
from mantid.simpleapi import DeleteWorkspace, mtd
from snapred.backend.dao.DetectorPeak import DetectorPeak
from snapred.backend.dao.GroupPeakList import GroupPeakList
from snapred.backend.dao.ingredients import DiffractionCalibrationIngredients as TheseIngredients
from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.dao.state.FocusGroup import FocusGroup
from snapred.backend.dao.state.InstrumentState import InstrumentState
from snapred.backend.recipe.algorithm.GroupDiffractionCalibration import GroupDiffractionCalibration
from snapred.backend.recipe.algorithm.PixelDiffractionCalibration import PixelDiffractionCalibration
from snapred.backend.recipe.DiffractionCalibrationRecipe import DiffractionCalibrationRecipe as ThisRecipe
from snapred.meta.Config import Resource

TheAlgorithmManager: str = "snapred.backend.recipe.DiffractionCalibrationRecipe.AlgorithmManager"


class TestDiffractionCalibtationRecipe(unittest.TestCase):
    def setUp(self):
        self.fakeRunNumber = "555"
        fakeRunConfig = RunConfig(runNumber=str(self.fakeRunNumber))

        fakeInstrumentState = InstrumentState.parse_raw(Resource.read("/inputs/calibration/sampleInstrumentState.json"))
        fakeInstrumentState.particleBounds.tof.minimum = 10
        fakeInstrumentState.particleBounds.tof.maximum = 1000

        fakeFocusGroup = FocusGroup.parse_raw(Resource.read("/inputs/diffcal/fakeFocusGroup.json"))
        fakeFocusGroup.definition = Resource.getPath("inputs/testInstrument/fakeSNAPFocGroup_Natural.xml")

        peakList = [
            DetectorPeak.parse_obj({"position": {"value": 1.5, "minimum": 1, "maximum": 2}}),
            DetectorPeak.parse_obj({"position": {"value": 3.5, "minimum": 3, "maximum": 4}}),
        ]

        self.fakeIngredients = TheseIngredients(
            runConfig=fakeRunConfig,
            focusGroup=fakeFocusGroup,
            instrumentState=fakeInstrumentState,
            groupedPeakLists=[
                GroupPeakList(groupID=3, peaks=peakList, maxfwhm=0.01),
                GroupPeakList(groupID=7, peaks=peakList, maxfwhm=0.02),
                GroupPeakList(groupID=2, peaks=peakList, maxfwhm=0.02),
                GroupPeakList(groupID=11, peaks=peakList, maxfwhm=0.02),
            ],
            convergenceThreshold=0.5,
            calPath=Resource.getPath("outputs/calibration/"),
        )
        self.recipe = ThisRecipe()

    def tearDown(self) -> None:
        workspaces = mtd.getObjectNames()
        # remove all workspaces
        for workspace in workspaces:
            try:
                DeleteWorkspace(workspace)
            except ValueError:
                print(f"Workspace {workspace} doesn't exist!")
        return super().tearDown

    # TODO: once recipe implemented, this should do something
    def test_chop_ingredients(self):
        self.recipe.chopIngredients(self.fakeIngredients)
        assert self.recipe.runNumber == self.fakeIngredients.runConfig.runNumber
        assert self.recipe.threshold == self.fakeIngredients.convergenceThreshold

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
                self.medianOffset: float = 4.0

            def PyExec(self):
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
        mock_AlgorithmManager.create.assert_called_once_with("PixelDiffractionCalibration")

    @mock.patch(TheAlgorithmManager)
    def test_execute_unsuccessful_later_calls(self, mock_AlgorithmManager):
        # a scoped dummy algorithm to test all three try/except blocks
        class DummyFailingAlgo(PythonAlgorithm):
            def PyInit(self):
                self.declareProperty("Ingredients", defaultValue="", direction=Direction.Input)
                self.declareProperty("OutputWorkspace", defaultValue="", direction=Direction.Output)
                self.declareProperty("InputWorkspace", defaultValue="", direction=Direction.Input)
                self.declareProperty("CalibrationTable", defaultValue="", direction=Direction.InOut)
                self.declareProperty("PreviousCalibrationTable", defaultValue="", direction=Direction.InOut)
                self.declareProperty("FinalCalibrationTable", defaultValue="", direction=Direction.InOut)
                self.declareProperty("fails", defaultValue=0, direction=Direction.Input)
                self.declareProperty("times", defaultValue=0, direction=Direction.InOut)
                self.declareProperty("data", defaultValue="", direction=Direction.Output)
                self.setRethrows(True)

            def PyExec(self):
                self.reexecute()

            def reexecute(self):
                fails = self.getProperty("fails").value
                times = self.getProperty("times").value
                self.setProperty("data", json.dumps({"medianOffset": 2 ** (-times)}))
                times += 1
                if times >= fails:
                    raise RuntimeError(f"passed {times}")
                self.setProperty("times", times)

        mockAlgo = DummyFailingAlgo()
        mockAlgo.initialize()
        mock_AlgorithmManager.create.return_value = mockAlgo

        for i in range(1, 4):
            mockAlgo.setProperty("fails", i)
            try:
                self.recipe.executeRecipe(self.fakeIngredients)
            except Exception as e:  # noqa: E722 BLE001
                assert str(e) == f"passed {i}"  # noqa: PT017
            else:
                # fail if execute did not raise an exception
                pytest.fail("Test should have raised RuntimeError, but no error raised")

    def makeFakeNeutronData(self, algo):
        """Will cause algorithm to execute with sample data, instead of loading from file"""
        from mantid.simpleapi import (
            ChangeBinOffset,
            CreateSampleWorkspace,
            LoadInstrument,
            MoveInstrumentComponent,
            RotateInstrumentComponent,
        )

        # prepare with test data
        CreateSampleWorkspace(
            OutputWorkspace=algo.inputWSdsp,
            # WorkspaceType="Histogram",
            Function="User Defined",
            UserDefinedFunction="name=Gaussian,Height=10,PeakCentre=1.2,Sigma=0.2",
            Xmin=algo.overallDMin,
            Xmax=algo.overallDMax,
            BinWidth=algo.dBin,
            XUnit="dSpacing",
            NumBanks=4,  # must produce same number of pixels as fake instrument
            BankPixelWidth=2,  # each bank has 4 pixels, 4 banks, 16 total
        )
        LoadInstrument(
            Workspace=algo.inputWSdsp,
            Filename=Resource.getPath("inputs/testInstrument/fakeSNAP.xml"),
            RewriteSpectraMap=True,
        )
        # the below are meant to de-align the pixels so an offset correction is needed
        ChangeBinOffset(
            InputWorkspace=algo.inputWSdsp,
            OutputWorkspace=algo.inputWSdsp,
            Offset=-0.7 * algo.overallDMin,
        )
        RotateInstrumentComponent(
            Workspace=algo.inputWSdsp,
            ComponentName="bank1",
            Y=1.0,
            Angle=90.0,
        )
        MoveInstrumentComponent(
            Workspace=algo.inputWSdsp,
            ComponentName="bank1",
            X=5.0,
            Y=-0.1,
            Z=0.1,
            RelativePosition=False,
        )
        # # rebin and convert for DSP, TOF
        algo.convertUnitsAndRebin(algo.inputWSdsp, algo.inputWSdsp, "dSpacing")
        algo.convertUnitsAndRebin(algo.inputWSdsp, algo.inputWStof, "TOF")

    def test_execute_with_algos(self):
        import os

        # create sample data
        offsetAlgo = PixelDiffractionCalibration()
        offsetAlgo.initialize()
        offsetAlgo.chopIngredients(self.fakeIngredients)
        self.makeFakeNeutronData(offsetAlgo)
        pdcalAlgo = GroupDiffractionCalibration()
        pdcalAlgo.initialize()
        pdcalAlgo.chopIngredients(self.fakeIngredients)
        fakeFile = pdcalAlgo.outputFilename
        res = self.recipe.executeRecipe(self.fakeIngredients)
        assert res["result"]
        print(res["steps"])
        assert res["steps"][-1]["medianOffset"] <= self.fakeIngredients.convergenceThreshold
        os.remove(fakeFile)


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
