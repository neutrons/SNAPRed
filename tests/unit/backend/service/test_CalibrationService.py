# ruff: noqa: E402, ARG002
import os
import tempfile
import unittest
import unittest.mock as mock
from typing import List
from unittest.mock import ANY, MagicMock, call, patch

import pytest
from mantid.simpleapi import (
    CreateWorkspace,
    mtd,
)
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceNameGenerator as wng

# Mock out of scope modules before importing DataExportService

localMock = mock.Mock()

with mock.patch.dict(
    "sys.modules",
    {
        "snapred.backend.data.DataExportService": mock.Mock(),
        "snapred.backend.data.DataFactoryService": mock.Mock(),
        "snapred.backend.log": mock.Mock(),
        "snapred.backend.log.logger": mock.Mock(),
    },
):
    from snapred.backend.dao import Limit
    from snapred.backend.dao.calibration.CalibrationIndexEntry import CalibrationIndexEntry
    from snapred.backend.dao.calibration.CalibrationMetric import CalibrationMetric
    from snapred.backend.dao.calibration.CalibrationRecord import CalibrationRecord
    from snapred.backend.dao.calibration.FocusGroupMetric import FocusGroupMetric
    from snapred.backend.dao.ingredients.ReductionIngredients import ReductionIngredients
    from snapred.backend.dao.request.CalibrationAssessmentRequest import CalibrationAssessmentRequest
    from snapred.backend.dao.request.DiffractionCalibrationRequest import DiffractionCalibrationRequest
    from snapred.backend.dao.request.FarmFreshIngredients import FarmFreshIngredients
    from snapred.backend.dao.RunConfig import RunConfig
    from snapred.backend.dao.state import PixelGroup
    from snapred.backend.dao.state.FocusGroup import FocusGroup
    from snapred.backend.recipe.DiffractionCalibrationRecipe import DiffractionCalibrationRecipe
    from snapred.backend.service.CalibrationService import CalibrationService
    from snapred.backend.service.SousChef import SousChef
    from snapred.meta.Config import Config, Resource

    thisService = "snapred.backend.service.CalibrationService."

    def readReductionIngredientsFromFile():
        with Resource.open("/inputs/calibration/ReductionIngredients.json", "r") as f:
            return ReductionIngredients.parse_raw(f.read())

    # test export calibration
    def test_exportCalibrationIndex():
        calibrationService = CalibrationService()
        calibrationService.dataExportService.exportCalibrationIndexEntry = mock.Mock()
        calibrationService.dataExportService.exportCalibrationIndexEntry.return_value = "expected"
        calibrationService.saveCalibrationToIndex(CalibrationIndexEntry(runNumber="1", comments="", author=""))
        assert calibrationService.dataExportService.exportCalibrationIndexEntry.called
        savedEntry = calibrationService.dataExportService.exportCalibrationIndexEntry.call_args.args[0]
        assert savedEntry.appliesTo == ">1"
        assert savedEntry.timestamp is not None

    def test_save():
        calibrationService = CalibrationService()
        calibrationService.dataExportService.exportCalibrationRecord = mock.Mock()
        calibrationService.dataExportService.exportCalibrationRecord.return_value = MagicMock(version="1.0.0")
        calibrationService.dataExportService.exportCalibrationIndexEntry = mock.Mock()
        calibrationService.dataExportService.exportCalibrationIndexEntry.return_value = "expected"
        calibrationService.save(mock.Mock())
        assert calibrationService.dataExportService.exportCalibrationRecord.called
        savedEntry = calibrationService.dataExportService.exportCalibrationRecord.call_args.args[0]
        assert savedEntry.parameters is not None

    def test_load():
        calibrationService = CalibrationService()
        calibrationService.dataFactoryService.getCalibrationRecord = mock.Mock(return_value="passed")
        res = calibrationService.load(mock.Mock())
        assert res == calibrationService.dataFactoryService.getCalibrationRecord.return_value


from snapred.backend.service.CalibrationService import CalibrationService  # noqa: F811


class TestCalibrationServiceMethods(unittest.TestCase):
    def setUp(self):
        self.instance = CalibrationService()
        self.runId = "test_run_id"
        self.outputNameFormat = "{}_calibration_reduction_result"
        # Mock the _calculatePixelGroupingParameters method to return a predefined value
        self.instance._calculatePixelGroupingParameters = lambda calib, focus_def, lite_mode, nBins: {  # noqa: ARG005
            "parameters": f"params for {focus_def}",
            "tof": f"tof for {focus_def}",
        }

    def clearoutWorkspaces(self) -> None:
        # Delete the workspaces created by loading
        for ws in mtd.getObjectNames():
            self.instance.dataFactoryService.deleteWorkspace(ws)

    def tearDown(self) -> None:
        # At the end of each test, clear out the workspaces
        self.clearoutWorkspaces()
        return super().tearDown()

    @patch(thisService + "parse_raw_as")
    @patch(thisService + "CalibrationMetricExtractionRecipe")
    @patch(thisService + "FocusGroupMetric")
    def test_collectMetrics(
        self,
        FocusGroupMetric,
        CalibrationMetricExtractionRecipe,
        parse_raw_as,
    ):
        # mock the inputs to the function
        focussedData = MagicMock(spec_set=str)
        focusGroup = MagicMock(spec=FocusGroup)
        focusGroup.name = "group1"
        pixelGroup = MagicMock(spec_set=PixelGroup)

        # mock external method calls
        parse_raw_as.side_effect = lambda x, y: y  # noqa: ARG005
        mockMetric = mock.Mock()
        CalibrationMetricExtractionRecipe.return_value.executeRecipe.return_value = mockMetric

        # Call the method to test
        metric = self.instance._collectMetrics(focussedData, focusGroup, pixelGroup)

        # Assert correct behavior
        assert CalibrationMetricExtractionRecipe.called_once_with(
            InputWorkspace=focussedData,
            PixelGroup=pixelGroup,
        )
        assert parse_raw_as.called_once_with(List[CalibrationMetric], mockMetric)
        assert FocusGroupMetric.called_once_with(
            focusGroupName=focusGroup.name,
            calibrationMetric=mockMetric,  # NOTE this is because of above lambda
        )
        assert metric == FocusGroupMetric.return_value

    @patch(thisService + "CalibrationRecord", return_value=MagicMock(mockId="mock_calibration_record"))
    @patch(thisService + "FitMultiplePeaksIngredients")
    @patch(thisService + "FitMultiplePeaksRecipe")
    @patch(thisService + "FarmFreshIngredients")
    @patch(
        thisService + "CalibrationMetricsWorkspaceIngredients",
        return_value=MagicMock(
            calibrationRecord=CalibrationRecord.parse_raw(Resource.read("inputs/calibration/CalibrationRecord.json")),
            timestamp="123",
        ),
    )
    def test_assessQuality(
        self,
        CalibrationMetricsWorkspaceIngredients,
        FarmFreshIngredients,
        FitMultiplePeaksRecipe,
        FitMultiplePeaksIngredients,
        CalibrationRecord,
    ):
        # Mock input data
        fakeFocusGroup = FocusGroup(name="egg", definition="muffin")
        fakeMetrics = CalibrationMetric(
            sigmaAverage=0.0,
            sigmaStandardDeviation=0.0,
            strainAverage=0.0,
            strainStandardDeviation=0.0,
            twoThetaAverage=0.0,
        )
        fakeMetrics = FocusGroupMetric(focusGroupName=fakeFocusGroup.name, calibrationMetric=[fakeMetrics])

        # Mock the necessary method calls
        self.instance.sousChef.prepPeakIngredients = MagicMock()
        self.instance.sousChef.prepCalibration = MagicMock()
        self.instance.dataFactoryService.getCifFilePath = MagicMock(return_value="good/cif/path")
        self.instance._collectMetrics = MagicMock(return_value=fakeMetrics)

        # Call the method to test
        request = CalibrationAssessmentRequest(
            workspace="mock workspace",
            run=RunConfig(runNumber="123"),
            useLiteMode=True,
            focusGroup={"name": fakeMetrics.focusGroupName, "definition": ""},
            calibrantSamplePath="egg/muffin/biscuit",
        )
        record = self.instance.assessQuality(request)

        # Assert correct method calls
        assert FarmFreshIngredients.call_count == 1
        assert self.instance.sousChef.prepPeakIngredients.called_once_with(FarmFreshIngredients.return_value)
        assert FitMultiplePeaksRecipe.called_once_with(FitMultiplePeaksIngredients.return_value)

        # Assert the result is as expected
        assert record == CalibrationRecord.return_value

        # Assert expected calibration metric workspaces have been generated
        for metric in ["sigma", "strain"]:
            ws_name = wng.diffCalMetrics().runNumber("57514").version("ts123").metricName(metric).build()
            assert self.instance.dataFactoryService.workspaceDoesExist(ws_name)

    def test_readQuality_no_calibration_record_exception(self):
        self.instance.dataFactoryService.getCalibrationRecord = MagicMock(return_value=None)
        mockRequest = MagicMock(runId=MagicMock(), version=MagicMock(), checkExistent=False)
        with pytest.raises(ValueError) as excinfo:  # noqa: PT011
            self.instance.readQuality(mockRequest)
        assert str(mockRequest.runId) in str(excinfo.value)
        assert str(mockRequest.version) in str(excinfo.value)

    @patch(thisService + "CalibrationMetricsWorkspaceIngredients", return_value=MagicMock())
    def test_readQuality_no_calibration_metrics_exception(
        self,
        mockCalibrationMetricsWorkspaceIngredients,
    ):
        mockRequest = MagicMock(runId=MagicMock(), version=MagicMock(), checkExistent=False)
        calibRecord = CalibrationRecord.parse_raw(Resource.read("inputs/calibration/CalibrationRecord.json"))
        self.instance.dataFactoryService.getCalibrationRecord = MagicMock(return_value=calibRecord)
        with pytest.raises(Exception) as excinfo:  # noqa: PT011
            self.instance.readQuality(mockRequest)
        assert "The input table is empty" in str(excinfo.value)

    def test_readQuality_check_existent(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            calibRecord = CalibrationRecord.parse_raw(Resource.read("inputs/calibration/CalibrationRecord.json"))
            self.instance.dataFactoryService.getCalibrationRecord = MagicMock(return_value=calibRecord)

            # Under a mocked calibration data path, create fake "persistent" workspace files
            self.instance.dataFactoryService.getCalibrationDataPath = MagicMock(return_value=tmpdir)
            for ws_name in calibRecord.workspaceNames:
                CreateWorkspace(
                    OutputWorkspace=ws_name,
                    DataX=1,
                    DataY=1,
                )
                self.instance.dataFactoryService.writeWorkspace(os.path.join(tmpdir, ws_name + ".nxs"), ws_name)

            # Call the method to test. Use a mocked run and a mocked version
            runId = MagicMock()
            version = MagicMock()
            mockRequest = MagicMock(runId=runId, version=version, checkExistent=False)
            self.instance.readQuality(mockRequest)
            with pytest.raises(ValueError) as excinfo:  # noqa: PT011
                self.instance.readQuality(MagicMock(runId=runId, version=version, checkExistent=True))
            assert "is already loaded" in str(excinfo.value)
            with pytest.raises(ValueError) as excinfo:  # noqa: PT011
                self.instance.readQuality(MagicMock(runId="57514", version="7", checkExistent=True))
            assert "is already loaded" in str(excinfo.value)

    def test_readQuality(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            calibRecord = CalibrationRecord.parse_raw(Resource.read("inputs/calibration/CalibrationRecord.json"))
            self.instance.dataFactoryService.getCalibrationRecord = MagicMock(return_value=calibRecord)

            # Under a mocked calibration data path, create fake "persistent" workspace files
            self.instance.dataFactoryService.getCalibrationDataPath = MagicMock(return_value=tmpdir)
            for ws_name in calibRecord.workspaceNames:
                CreateWorkspace(
                    OutputWorkspace=ws_name,
                    DataX=1,
                    DataY=1,
                )
                self.instance.dataFactoryService.writeWorkspace(os.path.join(tmpdir, ws_name + ".nxs"), ws_name)

            # Call the method to test. Use a mocked run and a mocked version
            mockRequest = MagicMock(runId=MagicMock(), version=MagicMock(), checkExistent=False)
            self.instance.readQuality(mockRequest)

            # Assert the expected calibration metric workspaces have been generated
            for metric in ["sigma", "strain"]:
                ws_name = (
                    wng.diffCalMetrics()
                    .runNumber(calibRecord.runNumber)
                    .version(calibRecord.version)
                    .metricName(metric)
                    .build()
                )
                assert self.instance.dataFactoryService.workspaceDoesExist(ws_name)

            # Assert the "persistent" workspaces have been loaded
            for ws_name in calibRecord.workspaceNames:
                assert self.instance.dataFactoryService.workspaceDoesExist(ws_name)

    @patch(thisService + "FarmFreshIngredients", spec_set=FarmFreshIngredients)
    @patch(thisService + "DiffractionCalibrationRecipe", spec_set=DiffractionCalibrationRecipe)
    def test_diffractionCalibration(
        self,
        DiffractionCalibrationRecipe,
        FarmFreshIngredients,
    ):
        runNumber = "123"
        focusGroup = FocusGroup(name="biscuit", definition="flour/butter")
        nBinsAcrossPeakWidth = 10

        self.instance.dataFactoryService.getCifFilePath = mock.Mock(return_value="bundt/cake.egg")

        mockIngredients = {"apple": "banana"}
        self.instance.sousChef = mock.Mock(spec_set=SousChef)
        self.instance.sousChef.prepDiffractionCalibrationIngredients.return_value = mockIngredients

        DiffractionCalibrationRecipe().executeRecipe.return_value = {"calibrationTable": "fake"}

        self.instance.groceryService.fetchGroceryDict = mock.Mock(return_value={"grocery1": "orange"})

        # Call the method with the provided parameters
        request = DiffractionCalibrationRequest(
            runNumber=runNumber,
            focusGroup=focusGroup,
            nBinsAcrossPeakWidth=nBinsAcrossPeakWidth,
            calibrantSamplePath="path/to/cif",
            useLiteMode=False,
        )
        self.instance.diffractionCalibration(request)

        # Perform assertions to check the result and method calls
        assert FarmFreshIngredients.call_count == 1
        assert self.instance.sousChef.prepDiffractionCalibrationIngredients.called_once_with(
            FarmFreshIngredients.return_value
        )
        assert DiffractionCalibrationRecipe().executeRecipe.called_once_with(
            self.instance.sousChef.prepDiffractionCalibrationIngredients.return_value,
            self.instance.groceryService.fetchGroceryDict.return_value,
        )
