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

# Mock out of scope modules before importing DataExportService

localMock = mock.Mock()

with mock.patch.dict(
    "sys.modules",
    {
        "snapred.backend.data.DataExportService": mock.Mock(),
        "snapred.backend.data.DataFactoryService": mock.Mock(),
        "snapred.backend.recipe.CalibrationReductionRecipe": mock.Mock(),
        "snapred.backend.log": mock.Mock(),
        "snapred.backend.log.logger": mock.Mock(),
    },
):
    from snapred.backend.dao import GroupPeakList, Limit
    from snapred.backend.dao.calibration.CalibrationIndexEntry import CalibrationIndexEntry
    from snapred.backend.dao.calibration.CalibrationMetric import CalibrationMetric
    from snapred.backend.dao.calibration.CalibrationRecord import CalibrationRecord
    from snapred.backend.dao.calibration.FocusGroupMetric import FocusGroupMetric
    from snapred.backend.dao.ingredients import CalibrationMetricsWorkspaceIngredients
    from snapred.backend.dao.ingredients.GroceryListItem import GroceryListItem
    from snapred.backend.dao.ingredients.ReductionIngredients import ReductionIngredients
    from snapred.backend.dao.ingredients.SmoothDataExcludingPeaksIngredients import (
        SmoothDataExcludingPeaksIngredients,
    )
    from snapred.backend.dao.normalization.NormalizationIndexEntry import NormalizationIndexEntry
    from snapred.backend.dao.normalization.NormalizationRecord import NormalizationRecord
    from snapred.backend.dao.request.DiffractionCalibrationRequest import DiffractionCalibrationRequest
    from snapred.backend.dao.request.NormalizationCalibrationRequest import (
        NormalizationCalibrationRequest,
    )
    from snapred.backend.dao.RunConfig import RunConfig
    from snapred.backend.dao.state import FocusGroup, PixelGroup, PixelGroupingParameters
    from snapred.backend.dao.state.CalibrantSample.CalibrantSamples import CalibrantSamples
    from snapred.backend.dao.state.CalibrantSample.Geometry import Geometry
    from snapred.backend.dao.state.CalibrantSample.Material import Material
    from snapred.backend.recipe.PixelGroupingParametersCalculationRecipe import (
        PixelGroupingParametersCalculationRecipe,
    )
    from snapred.backend.service.CalibrationService import CalibrationService
    from snapred.meta.Config import Resource

    thisService = "snapred.backend.service.CalibrationService."

    def readReductionIngredientsFromFile():
        with Resource.open("/inputs/calibration/input.json", "r") as f:
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

    def test_exportNormalizationIndexEntry():
        calibrationService = CalibrationService()
        calibrationService.dataExportService.exportNormalizationIndexEntry = MagicMock()
        calibrationService.dataExportService.exportNormalizationIndexEntry.return_value = "expected"
        calibrationService.saveNormalizationToIndex(NormalizationIndexEntry(runNumber="1", backgroundRunNumber="2"))
        assert calibrationService.dataExportService.exportNormalizationIndexEntry.called
        savedEntry = calibrationService.dataExportService.exportNormalizationIndexEntry.call_args.args[0]
        assert savedEntry.appliesTo == ">1"
        assert savedEntry.timestamp is not None

    def test_saveNormalization():
        calibrationService = CalibrationService()
        calibrationService.dataExportService.exportNormalizationRecord = mock.Mock()
        calibrationService.dataExportService.exportNormalizationRecord.return_value = MagicMock(version="1.0.0")
        calibrationService.dataExportService.exportNormalizationIndexEntry = mock.Mock()
        calibrationService.dataExportService.exportNormalizationIndexEntry.return_value = "expected"
        calibrationService.dataFactoryService.getReductionIngredients = mock.Mock()
        calibrationService.dataFactoryService.getReductionIngredients.return_value = readReductionIngredientsFromFile()
        calibrationService.saveNormalization(mock.Mock())
        assert calibrationService.dataExportService.exportNormalizationRecord.called
        savedEntry = calibrationService.dataExportService.exportNormalizationRecord.call_args.args[0]
        assert savedEntry.parameters is not None

    def test_save():
        calibrationService = CalibrationService()
        calibrationService.dataExportService.exportCalibrationRecord = mock.Mock()
        calibrationService.dataExportService.exportCalibrationRecord.return_value = MagicMock(version="1.0.0")
        calibrationService.dataExportService.exportCalibrationIndexEntry = mock.Mock()
        calibrationService.dataExportService.exportCalibrationIndexEntry.return_value = "expected"
        calibrationService.dataFactoryService.getReductionIngredients = mock.Mock()
        calibrationService.dataFactoryService.getReductionIngredients.return_value = readReductionIngredientsFromFile()
        calibrationService.save(mock.Mock())
        assert calibrationService.dataExportService.exportCalibrationRecord.called
        savedEntry = calibrationService.dataExportService.exportCalibrationRecord.call_args.args[0]
        assert savedEntry.parameters is not None

    def test_load():
        calibrationService = CalibrationService()
        calibrationService.dataFactoryService.getCalibrationRecord = mock.Mock(return_value="passed")
        res = calibrationService.load(mock.Mock())
        assert res == calibrationService.dataFactoryService.getCalibrationRecord.return_value

    # test calculate pixel grouping parameters
    # patch datafactoryservice, pixelgroupingparameterscalculationrecipe, pixelgroupingingredients, dataExportService
    @mock.patch(thisService + "GroceryService")
    @mock.patch(thisService + "DataFactoryService", return_value=mock.Mock())
    @mock.patch(thisService + "PixelGroupingParametersCalculationRecipe")
    @mock.patch(thisService + "PixelGroupingIngredients")
    @mock.patch(thisService + "DataExportService")
    def test_calculatePixelGroupingParameters(
        mockDataExportService,  # noqa: ARG001
        mockPixelGroupingParametersCalculationRecipe,
        mockPixelGroupingIngredients,  # noqa: ARG001
        mockDataFactoryService,  # noqa: ARG001
        mockGroceryService,  # noqa: ARG001
    ):
        runs = [RunConfig(runNumber="1")]
        calibrationService = CalibrationService()
        groupingFile = "junk/SNAPFocGroup_Column.abc"
        useLiteMode = True
        setattr(PixelGroupingParametersCalculationRecipe, "executeRecipe", localMock)
        localMock.return_value = mock.MagicMock()
        calibrationService.calculatePixelGroupingParameters(runs, groupingFile, useLiteMode)
        assert mockGroceryService.return_value.fetchGroceryList.called
        assert mockPixelGroupingParametersCalculationRecipe.called


from snapred.backend.service.CalibrationService import CalibrationService  # noqa: F811


class TestCalibrationServiceMethods(unittest.TestCase):
    def setUp(self):
        self.instance = CalibrationService()
        self.runId = "test_run_id"
        self.outputNameFormat = "{}_calibration_reduction_result"
        # Mock the _calculatePixelGroupingParameters method to return a predefined value
        self.instance._calculatePixelGroupingParameters = lambda calib, focus_def, lite_mode: {  # noqa: ARG005
            "parameters": f"params for {focus_def}"
        }

    def clearoutWorkspaces(self) -> None:
        # Delete the workspaces created by loading
        for ws in mtd.getObjectNames():
            self.instance.dataFactoryService.deleteWorkspace(ws)

    def tearDown(self) -> None:
        # At the end of each test, clear out the workspaces
        self.clearoutWorkspaces()
        return super().tearDown()

    def test_GetPixelGroupingParams(self):
        # Mock input data
        calibration = "calibration_data"
        focusGroups = [MagicMock(definition="focus1"), MagicMock(definition="focus2"), MagicMock(definition="focus3")]
        useLiteMode = mock.Mock()

        # Call the method to test
        result = self.instance._getPixelGroupingParams(calibration, focusGroups, useLiteMode)

        # Assert the result is as expected
        expected_result = ["params for focus1", "params for focus2", "params for focus3"]
        assert result == expected_result

    def test_GetPixelGroupingParams_Empty(self):
        # Test with an empty focusGroups list
        calibration = "calibration_data"
        focusGroups = []
        useLiteMode = mock.Mock()

        # Call the method to test
        result = self.instance._getPixelGroupingParams(calibration, focusGroups, useLiteMode)

        # Assert the result is an empty list
        assert result == []

    @patch(thisService + "GroupWorkspaceIterator", return_value=["ws1", "ws2"])
    @patch(thisService + "FitCalibrationWorkspaceIngredients", side_effect=["fit_result_1", "fit_result_2"])
    @patch(
        thisService + "FitMultiplePeaksRecipe",
        return_value=MagicMock(executeRecipe=MagicMock(side_effect=["fit_result_1", "fit_result_2"])),
    )
    @patch(
        thisService + "CalibrationMetricExtractionRecipe",
        return_value=MagicMock(executeRecipe=MagicMock("dsfdfad")),
    )
    @patch(thisService + "list_to_raw", return_value="mock_metric")
    @patch(thisService + "parse_raw_as", return_value="mock_crystal_info")
    @patch(thisService + "FocusGroupMetric", return_value="mock_metric")
    def test_fitAndCollectMetrics(
        self,
        groupWorkspaceIterator,
        fitCalibrationWorkspaceIngredients,
        fitCalibrationWorkspaceRecipe,
        calibrationMetricExtractionRecipe,
        list_to_raw_mock,
        parse_raw_as_mock,
        metricTypeMock,
    ):
        fakeFocussedData = "mock_focussed_data"
        fakeFocusGroups = MagicMock(name="group1")
        fakePixelGroupingParams = [
            PixelGroupingParameters(
                groupID=1, twoTheta=0.5, dResolution=Limit(minimum=1, maximum=2), dRelativeResolution=0.5
            )
        ]

        # Mock the necessary method calls
        mockGroupWorkspaceIterator = MagicMock(return_value=["ws1", "ws2"])
        # FitCalibrationWorkspaceRecipe = MagicMock(side_effect=["fit_result_1", "fit_result_2"])
        # fitCalibrationWorkspaceRecipe.executeRecipe = MagicMock(return_value="fit_result")
        mockCalibrationMetricExtractionRecipe = MagicMock(return_value="mock_metric")
        self.instance.GroupWorkspaceIterator = mockGroupWorkspaceIterator
        self.instance.CalibrationMetricExtractionRecipe = mockCalibrationMetricExtractionRecipe

        # Call the method to test
        metric = self.instance._collectMetrics(fakeFocussedData, fakeFocusGroups, fakePixelGroupingParams)

        assert metric == "mock_metric"

    @patch(thisService + "PixelGroup")
    def test_collectPixelGroups(self, mockPixelGroup):
        numItems = 4
        mockPixelGroup.side_effect = range(numItems)
        mockFocusGroups = [mock.Mock()] * numItems
        mockPGPs = [mock.Mock()] * numItems
        nBinsAcross = 42

        pgs = self.instance.collectPixelGroups(mockFocusGroups, mockPGPs, nBinsAcross)
        assert mockPixelGroup.call_count == numItems
        calls = [call(mockFocusGroups[i], mockPGPs[i], nBinsAcross) for i in range(numItems)]
        assert mockPixelGroup.has_calls(calls)
        assert pgs == list(range(numItems))

    @patch(
        thisService + "CrystallographicInfoService",
        return_value=MagicMock(ingest=MagicMock(return_value={"crystalInfo": "mock_crystal_info"})),
    )
    @patch(thisService + "CalibrationRecord", return_value=MagicMock(mockId="mock_calibration_record"))
    @patch(thisService + "FitMultiplePeaksIngredients")
    @patch(thisService + "FitMultiplePeaksRecipe")
    @patch(
        thisService + "CalibrationMetricsWorkspaceIngredients",
        return_value=MagicMock(
            calibrationRecord=CalibrationRecord.parse_raw(Resource.read("inputs/calibration/CalibrationRecord.json")),
            timestamp="123",
        ),
    )
    def test_assessQuality(
        self,
        mockCrystalInfoService,
        mockCalibRecord,
        fitMultiplePeaksIng,
        fmprecipe,
        mockCalibrationMetricsWorkspaceIngredients,
    ):
        # Mock input data
        mockRequest = MagicMock()
        mockRun = MagicMock()
        fakeCalibrantSamplePath = "mock_cif_path"
        mockReductionIngredients = MagicMock()
        mockCalibration = MagicMock()
        mockInstrumentState = MagicMock()
        fakeFocussedData = "mock_focussed_data"
        fakePixelGroupingParams = [
            PixelGroupingParameters(
                groupID=1, twoTheta=0.5, dResolution=Limit(minimum=1, maximum=2), dRelativeResolution=0.5
            ),
            PixelGroupingParameters(
                groupID=2, twoTheta=0.3, dResolution=Limit(minimum=2, maximum=3), dRelativeResolution=0.3
            ),
        ]
        fakeMetrics = CalibrationMetric(
            sigmaAverage=0.0,
            sigmaStandardDeviation=0.0,
            strainAverage=0.0,
            strainStandardDeviation=0.0,
            twoThetaAverage=0.0,
        )
        fakeMetrics = FocusGroupMetric(focusGroupName="group1", calibrationMetric=[fakeMetrics])
        fakeFocusGroupParameters = {"focus1": {"param1": 1}, "focus2": {"param2": 2}}

        # Mock the necessary method calls
        mockRequest.run = mockRun
        mockRequest.calibrantSamplePath = fakeCalibrantSamplePath
        mockRequest.smoothingParameter = 0.5
        self.instance.dataFactoryService.getReductionIngredients = MagicMock(return_value=mockReductionIngredients)
        self.instance.dataFactoryService.getCalibrationState = MagicMock(return_value=mockCalibration)
        self.instance.dataFactoryService.getCalibrationRecord = MagicMock(return_value=mockCalibRecord)
        self.instance.dataFactoryService.getCifFilePath = MagicMock(return_value=fakeCalibrantSamplePath)
        mockCalibration.instrumentState = mockInstrumentState
        self.instance._loadFocusedData = MagicMock(return_value=fakeFocussedData)
        self.instance._getPixelGroupingParams = MagicMock(return_value=fakePixelGroupingParams)
        self.instance._collectMetrics = MagicMock(return_value=fakeMetrics)
        self.instance.collectPixelGroups = MagicMock(return_value=fakeFocusGroupParameters)
        self.instance._generateFocusGroupAndInstrumentState = MagicMock(return_value=(MagicMock(), mockInstrumentState))

        # Call the method to test
        result = self.instance.assessQuality(mockRequest)

        # Assert the result is as expected
        expected_record_mockId = "mock_calibration_record"
        assert result.mockId == expected_record_mockId

        # Assert expected calibration metric workspaces have been generated
        ws_name_stem = "57514_calibrationMetrics_ts123"
        for metric in ["sigma", "strain"]:
            assert self.instance.dataFactoryService.workspaceDoesExist(ws_name_stem + "_" + metric)

    def test_readQuality_no_calibration_record_exception(self):
        self.instance.dataFactoryService.getCalibrationRecord = MagicMock(return_value=None)
        run = MagicMock()
        version = MagicMock()
        with pytest.raises(ValueError) as excinfo:  # noqa: PT011
            self.instance.readQuality(run, version)
        assert str(run) in str(excinfo.value)
        assert str(version) in str(excinfo.value)

    @patch(thisService + "CalibrationMetricsWorkspaceIngredients", return_value=MagicMock())
    def test_readQuality_no_calibration_metrics_exception(
        self,
        mockCalibrationMetricsWorkspaceIngredients,
    ):
        run = MagicMock()
        version = MagicMock()
        calibRecord = CalibrationRecord.parse_raw(Resource.read("inputs/calibration/CalibrationRecord.json"))
        self.instance.dataFactoryService.getCalibrationRecord = MagicMock(return_value=calibRecord)
        with pytest.raises(Exception) as excinfo:  # noqa: PT011
            self.instance.readQuality(run, version)
        assert "The input table is empty" in str(excinfo.value)

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
            run = MagicMock()
            version = MagicMock()
            self.instance.readQuality(run, version)

            # Assert the expected calibration metric workspaces have been generated
            ws_name_stem = f"{calibRecord.runNumber}_calibrationMetrics_v{calibRecord.version}"
            for ws_name in [ws_name_stem + "_sigma", ws_name_stem + "_strain"]:
                assert self.instance.dataFactoryService.workspaceDoesExist(ws_name)

            # Assert the "persistent" workspaces have been loaded
            for ws_name in calibRecord.workspaceNames:
                assert self.instance.dataFactoryService.workspaceDoesExist(ws_name)

    # patch FocusGroup
    @patch(thisService + "FocusGroup", return_value=FocusGroup(name="mockgroup", definition="junk/mockgroup.abc"))
    def test__generateFocusGroupAndInstrumentState(self, mockFocusGroup):
        # mock datafactoryservice.getCalibrationState
        self.instance.dataFactoryService.getCalibrationState = MagicMock()
        # mock self._calculatePixelGroupingParameters
        self.instance._calculatePixelGroupingParameters = MagicMock()
        # mock calibration.instrumentState from getCalibrationState
        mockInstrumentState = MagicMock()
        self.instance.dataFactoryService.getCalibrationState.return_value.instrumentState = mockInstrumentState

        actualFocusGroup, actualInstrumentState = self.instance._generateFocusGroupAndInstrumentState(
            MagicMock(),
            MagicMock(),
            MagicMock(),
        )
        assert mockFocusGroup.called
        assert actualFocusGroup == mockFocusGroup.return_value
        assert actualInstrumentState == mockInstrumentState

    @patch("snapred.backend.service.CalibrationService.CrystallographicInfoService")
    @patch("snapred.backend.service.CalibrationService.DetectorPeakPredictorRecipe")
    @patch("snapred.backend.service.CalibrationService.parse_raw_as")
    @patch("snapred.backend.service.CalibrationService.DiffractionCalibrationRecipe")
    @patch("snapred.backend.service.CalibrationService.DiffractionCalibrationIngredients")
    def test_diffractionCalibration(
        self,
        mockDiffractionCalibrationIngredients,
        mockDiffractionCalibrationRecipe,
        mockParseRawAs,
        mockDetectorPeakPredictorRecipe,
        mockCrystallographicInfoService,
    ):
        runNumber = "123"
        focusGroupPath = "focus/group/path"
        nBinsAcrossPeakWidth = 10
        self.instance.dataFactoryService = MagicMock()
        # Mocking dependencies and their return values
        self.instance.dataFactoryService.getRunConfig.return_value = MagicMock()
        mockParseRawAs.return_value = [MagicMock()]

        mockDiffractionCalibrationRecipe().executeRecipe.return_value = {"calibrationTable": "fake"}
        mockCrystallographicInfoService().ingest.return_value = {"crystalInfo": MagicMock()}

        mockGroceryDict = {"grocery1": mock.Mock(), "grocery2": mock.Mock()}
        self.instance.groceryService.fetchGroceryDict = mock.Mock(return_value=mockGroceryDict)

        mockFocusGroupInstrumentState = (MagicMock(), MagicMock())
        self.instance._generateFocusGroupAndInstrumentState = MagicMock(return_value=mockFocusGroupInstrumentState)
        mockFocusGroupInstrumentState[1].pixelGroup = mock.Mock()

        # Call the method with the provided parameters
        request = DiffractionCalibrationRequest(
            runNumber=runNumber,
            focusGroupPath=focusGroupPath,
            nBinsAcrossPeakWidth=nBinsAcrossPeakWidth,
            calibrantSamplePath="path/to/cif",
            useLiteMode=False,
        )
        self.instance.diffractionCalibration(request)

        # Perform assertions to check the result and method calls
        self.instance.dataFactoryService.getRunConfig.assert_called_once_with(runNumber)
        mockDetectorPeakPredictorRecipe().executeRecipe.assert_called_once_with(
            InstrumentState=mockFocusGroupInstrumentState[1],
            CrystalInfo=mockCrystallographicInfoService().ingest.return_value["crystalInfo"],
            PeakIntensityFractionThreshold=request.peakIntensityThreshold,
        )
        mockDiffractionCalibrationIngredients.assert_called_once_with(
            runConfig=self.instance.dataFactoryService.getRunConfig.return_value,
            instrumentState=mockFocusGroupInstrumentState[1],
            focusGroup=mockFocusGroupInstrumentState[0],
            groupedPeakLists=mockParseRawAs.return_value,
            calPath="~/tmp/",
            convergenceThreshold=request.convergenceThreshold,
            pixelGroup=mockFocusGroupInstrumentState[1].pixelGroup,
        )
        mockDiffractionCalibrationRecipe().executeRecipe.assert_called_once_with(
            mockDiffractionCalibrationIngredients.return_value,
            mockGroceryDict,
        )

    @patch(thisService + "CrystallographicInfoService")
    @patch(thisService + "SmoothDataExcludingPeaksIngredients")
    @patch(thisService + "NormalizationCalibrationIngredients")
    @patch(thisService + "CalibrationNormalizationRecipe")
    def test_calibrationNormalization(
        self,
        mockCalibrationNormalizationRecipe,
        mockNormalizationCalibrationIngredients,
        mockSmoothDataExcludingPeaksIngredients,
        mockCrystallographicInfoService,
    ):
        runNumber = "123"
        backgroundRunNumber = "124"
        useLiteMode = False

        # Mock the necessary method calls
        mockReductionIngredients = MagicMock()
        mockBackgroundReductionIngredients = MagicMock()
        mockCalibrantSample = MagicMock()
        mockSampleFilePath = MagicMock()
        self.instance.dataFactoryService = MagicMock()
        self.instance.dataFactoryService.getReductionIngredients = MagicMock(return_value=mockReductionIngredients)
        self.instance.dataFactoryService.getReductionIngredients = MagicMock(
            return_value=mockBackgroundReductionIngredients
        )
        self.instance.dataFactoryService.getCalibrantSamples = MagicMock(return_value=mockCalibrantSample)
        self.instance.dataFactoryService.getCifFilePath = MagicMock(return_value=mockSampleFilePath)
        mockCrystallographicInfoService().ingest.return_value = {"crystalInfo": MagicMock()}
        mockCalibrationNormalizationRecipe().executeRecipe.return_value = [MagicMock()]

        request = NormalizationCalibrationRequest(
            runNumber=runNumber,
            backgroundRunNumber=backgroundRunNumber,
            useLiteMode=useLiteMode,
            samplePath="",
            groupingPath="",
            smoothingParameter=0.5,
        )

        mockFocusGroupInstrumentState = (MagicMock(), MagicMock())
        self.instance._generateFocusGroupAndInstrumentState = MagicMock(return_value=mockFocusGroupInstrumentState)
        mockFocusGroupInstrumentState[1].pixelGroup = mock.Mock()

        mockGroceries = {"inputWorkspace": "fake1", "backgroundWorkspace": "fake2", "groupingWorkspace": "fake3"}
        self.instance.groceryService = mock.Mock()
        self.instance.groceryService.fetchGroceryDict = mock.Mock(return_value=mockGroceries)

        # Call the method to test TODO
        self.instance.normalization(request)

        # Perform assertions to check the result and method calls
        mockSmoothDataExcludingPeaksIngredients.assert_called_once_with(
            crystalInfo=mockCrystallographicInfoService().ingest.return_value["crystalInfo"],
            instrumentState=mockFocusGroupInstrumentState[1],
            smoothingParameter=request.smoothingParameter,
        )

        mockCalibrationNormalizationRecipe().executeRecipe.assert_called_once_with(
            mockNormalizationCalibrationIngredients.return_value,
            self.instance.groceryService.fetchGroceryDict.return_value,
        )

    @patch(thisService + "NormalizationRecord", return_value="mock_normalization_record")
    def test_normalizationAssessment(self, mockNormalizationRecord):
        mockRequest = MagicMock()
        mockRequest.runNumber = "58810"
        mockRequest.backgroundRunNumber = "58813"
        mockRequest.smoothingParameter = 0.1
        mockRequest.samplePath = "mock_cif_path"
        mockRequest.focusGroupPath = "mock_grouping_path"

        self.instance.dataFactoryService.getNormalizationState = MagicMock(return_value="mock_normalization_state")

        result = self.instance.normalizationAssessment(mockRequest)

        expected_record = "mock_normalization_record"
        assert result == expected_record
