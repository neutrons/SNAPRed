import unittest
import unittest.mock as mock
from typing import List
from unittest.mock import ANY, MagicMock, patch

import pytest

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
    from snapred.backend.dao.calibration.CalibrationIndexEntry import CalibrationIndexEntry  # noqa: E402
    from snapred.backend.dao.calibration.CalibrationMetric import CalibrationMetric  # noqa: E402
    from snapred.backend.dao.calibration.CalibrationRecord import CalibrationRecord  # noqa: E402
    from snapred.backend.dao.calibration.FocusGroupMetric import FocusGroupMetric  # noqa: E402
    from snapred.backend.dao.ingredients.ReductionIngredients import ReductionIngredients  # noqa: E402
    from snapred.backend.dao.ingredients.SmoothDataExcludingPeaksIngredients import (
        SmoothDataExcludingPeaksIngredients,  # noqa: E402
    )
    from snapred.backend.dao.normalization.NormalizationIndexEntry import NormalizationIndexEntry  # noqa: E402
    from snapred.backend.dao.normalization.NormalizationRecord import NormalizationRecord  # noqa: E402
    from snapred.backend.dao.request.DiffractionCalibrationRequest import DiffractionCalibrationRequest  # noqa: E402
    from snapred.backend.dao.request.NormalizationCalibrationRequest import (
        NormalizationCalibrationRequest,  # noqa: E402
    )
    from snapred.backend.dao.RunConfig import RunConfig  # noqa: E402
    from snapred.backend.dao.state import FocusGroup, PixelGroup, PixelGroupingParameters  # noqa: E402
    from snapred.backend.dao.state.CalibrantSample.CalibrantSamples import CalibrantSamples  # noqa: E402
    from snapred.backend.dao.state.CalibrantSample.Geometry import Geometry  # noqa: E402
    from snapred.backend.dao.state.CalibrantSample.Material import Material  # noqa: E402
    from snapred.backend.recipe.PixelGroupingParametersCalculationRecipe import (
        PixelGroupingParametersCalculationRecipe,  # noqa: E402
    )
    from snapred.backend.service.CalibrationService import CalibrationService  # noqa: E402
    from snapred.meta.Config import Resource  # noqa: E402

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
    @mock.patch(thisService + "FetchGroceriesRecipe")
    @mock.patch(thisService + "DataFactoryService", return_value=mock.Mock())
    @mock.patch(thisService + "PixelGroupingParametersCalculationRecipe")
    @mock.patch(thisService + "PixelGroupingIngredients")
    @mock.patch(thisService + "DataExportService")
    def test_calculatePixelGroupingParameters(
        mockFetchGroceries,  # noqa: ARG001
        mockDataFactoryService,  # noqa: ARG001
        mockPixelGroupingIngredients,  # noqa: ARG001
        mockPixelGroupingParametersCalculationRecipe,
        mockDataExportService,  # noqa: ARG001
    ):
        runs = [RunConfig(runNumber="1")]
        calibrationService = CalibrationService()
        groupingFile = "junk/SNAPFocGroup_Column.abc"
        useLiteMode = True
        setattr(PixelGroupingParametersCalculationRecipe, "executeRecipe", localMock)
        localMock.return_value = mock.MagicMock()
        calibrationService.calculatePixelGroupingParameters(runs, groupingFile, useLiteMode)
        assert mockPixelGroupingParametersCalculationRecipe.called


from snapred.backend.service.CalibrationService import CalibrationService  # noqa: E402, F811


class TestCalibrationServiceMethods(unittest.TestCase):
    def setUp(self):
        self.instance = CalibrationService()
        self.runId = "test_run_id"
        self.outputNameFormat = "{}_calibration_reduction_result"
        # Mock the _calculatePixelGroupingParameters method to return a predefined value
        self.instance._calculatePixelGroupingParameters = lambda calib, focus_def, lite_mode: {  # noqa: ARG005
            "parameters": f"params for {focus_def}"
        }

    def test_LoadFocussedData_Success(self):
        # Mock the dataFactoryService.getWorkspaceForName method to return a non-None value
        self.instance.dataFactoryService.getWorkspaceForName = MagicMock(return_value="mock_workspace")

        # Call the method to test
        result = self.instance._loadFocusedData(self.runId)

        # Assert the return value is the expected one
        expected_result = self.outputNameFormat.format(self.runId)
        assert result == expected_result

    def test_LoadFocussedData_Failure(self):
        # Mock the dataFactoryService.getWorkspaceForName method to return None
        self.instance.dataFactoryService.getWorkspaceForName = MagicMock(return_value=None)

        # Call the method to test and expect an Exception to be raised
        expected_message = "No focussed data found for run {}, Please run Calibration Reduction on this Data.".format(
            self.runId
        )
        with pytest.raises(ValueError, match=expected_message):
            self.instance._loadFocusedData(self.runId)

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
        groupWorkspaceIterator,  # noqa: ARG002
        fitCalibrationWorkspaceIngredients,  # noqa: ARG002
        fitCalibrationWorkspaceRecipe,  # noqa: ARG002
        calibrationMetricExtractionRecipe,  # noqa: ARG002
        list_to_raw_mock,  # noqa: ARG002
        parse_raw_as_mock,  # noqa: ARG002
        metricTypeMock,  # noqa: ARG002
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

    @patch(
        thisService + "CrystallographicInfoService",
        return_value=MagicMock(ingest=MagicMock(return_value={"crystalInfo": "mock_crystal_info"})),
    )
    @patch(thisService + "CalibrationRecord", return_value="mock_calibration_record")
    @patch(thisService + "FitMultiplePeaksIngredients")
    @patch(thisService + "FitMultiplePeaksRecipe")
    def test_assessQuality(
        self, mockCrystalInfoService, mockCalibRecord, fitMultiplePeaksIng, fmprecipe  # noqa: ARG002
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
        self.instance.collectFocusGroupParameters = MagicMock(return_value=fakeFocusGroupParameters)
        self.instance._generateFocusGroupAndInstrumentState = MagicMock(return_value=(MagicMock(), mockInstrumentState))

        # Call the method to test
        result = self.instance.assessQuality(mockRequest)

        # Assert the result is as expected
        expected_record = "mock_calibration_record"
        assert result == expected_record

    @patch("snapred.backend.service.CalibrationService.CalibrationRecord", return_value="mock_calibration_record")
    def test_readQuality(self, mockCalibRecord):
        MagicMock()
        run = MagicMock()
        version = MagicMock()
        self.instance.dataFactoryService.getCalibrationRecord = MagicMock(return_value=mockCalibRecord)
        # Call the method to test
        self.instance.readQuality(run, version)

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

    @patch(thisService + "GroceryListItem")
    @patch(thisService + "FetchGroceriesRecipe")
    @patch(thisService + "CrystallographicInfoService")
    @patch(thisService + "DetectorPeakPredictorRecipe")
    @patch(thisService + "parse_raw_as")
    @patch(thisService + "DiffractionCalibrationRecipe")
    @patch(thisService + "DiffractionCalibrationIngredients")
    def test_diffractionCalibration(
        self,
        mockDiffractionCalibrationIngredients,
        mockDiffractionCalibrationRecipe,
        mockParseRawAs,
        mockDetectorPeakPredictorRecipe,
        mockCrystallographicInfoService,
        mockFetchGroceriesRecipe,
        mockGroceryListItem,
    ):
        runNumber = "123"
        focusGroupPath = "focus/group/path"
        nBinsAcrossPeakWidth = 10
        self.instance.dataFactoryService = MagicMock()
        # Mocking dependencies and their return values
        self.instance.dataFactoryService.getRunConfig.return_value = MagicMock()
        mockParseRawAs.return_value = [MagicMock()]
        mockDetectorPeakPredictorRecipe().executeRecipe.return_value = [MagicMock()]
        mockCrystallographicInfoService().ingest.return_value = {"crystalInfo": MagicMock()}

        mockFetchGroceriesRecipe().executeRecipe.return_value = {
            "workspaces": [mock.Mock(), mock.Mock()],
        }

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
        assert mockGroceryListItem.call_count == 2
        mockGroceries = mockFetchGroceriesRecipe().executeRecipe.return_value["workspaces"]
        mockDiffractionCalibrationRecipe().executeRecipe.assert_called_once_with(
            mockDiffractionCalibrationIngredients.return_value,
            {"inputWorkspace": mockGroceries[0], "groupingWorkspace": mockGroceries[1]},
        )

    @patch(thisService + "GroceryListItem")
    @patch(thisService + "FetchGroceriesRecipe")
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
        mockFetchGroceriesRecipe,
        mockGroceryListItem,
    ):
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

        request = mock.MagicMock()

        mockFocusGroupInstrumentState = (MagicMock(), MagicMock())
        self.instance._generateFocusGroupAndInstrumentState = MagicMock(return_value=mockFocusGroupInstrumentState)
        mockFocusGroupInstrumentState[1].pixelGroup = mock.Mock()

        # Call the method to test TODO
        self.instance.normalization(request)

        # Perform assertions to check the result and method calls
        mockSmoothDataExcludingPeaksIngredients.assert_called_once_with(
            crystalInfo=mockCrystallographicInfoService().ingest.return_value["crystalInfo"],
            instrumentState=mockFocusGroupInstrumentState[1],
            smoothingParameter=request.smoothingParameter,
        )

        assert mockGroceryListItem.call_count == 3
        mockGroceries = mockFetchGroceriesRecipe().executeRecipe.return_value["workspaces"]

        mockCalibrationNormalizationRecipe().executeRecipe.assert_called_once_with(
            mockNormalizationCalibrationIngredients.return_value,
            {
                "inputWorkspace": mockGroceries[0],
                "backgroundWorkspace": mockGroceries[1],
                "groupingWorkspace": mockGroceries[2],
                "outputWorkspace": "focussedRawVanadium",
                "smoothedOutput": "smoothedOutput",
            },
        )

    @patch(thisService + "NormalizationRecord", return_value="mock_normalization_record")
    def test_normalizationAssessment(self, mockNormalizationRecord):  # noqa: ARG002
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
