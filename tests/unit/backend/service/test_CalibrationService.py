import unittest
import unittest.mock as mock
from unittest.mock import MagicMock, patch

# Mock out of scope modules before importing DataExportService

localMock = mock.Mock()

with mock.patch.dict(
    "sys.modules",
    {
        "snapred.backend.data.DataExportService": mock.Mock(),
        "snapred.backend.data.DataFactoryService": mock.Mock(),
        "snapred.backend.recipe.CalibrationReductionRecipe": mock.Mock(),
        "snapred.backend.dao.ingredients.PixelGroupingIngredients": mock.MagicMock(),
        "snapred.backend.log": mock.Mock(),
        "snapred.backend.log.logger": mock.Mock(),
    },
):
    from snapred.backend.dao.calibration.CalibrationIndexEntry import CalibrationIndexEntry  # noqa: E402
    from snapred.backend.dao.calibration.CalibrationRecord import CalibrationRecord  # noqa: E402
    from snapred.backend.dao.ingredients import ReductionIngredients  # noqa: E402
    from snapred.backend.dao.RunConfig import RunConfig  # noqa: E402
    from snapred.backend.recipe.PixelGroupingParametersCalculationRecipe import (
        PixelGroupingParametersCalculationRecipe,  # noqa: E402
    )
    from snapred.backend.service.CalibrationService import CalibrationService  # noqa: E402
    from snapred.meta.Config import Resource  # noqa: E402

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

    # test calculate pixel grouping parameters
    # patch datafactoryservice, pixelgroupingparameterscalculationrecipe, pixelgroupingingredients, dataExportService
    @mock.patch("snapred.backend.service.CalibrationService.DataFactoryService", return_value=mock.Mock())
    @mock.patch(
        "snapred.backend.service.CalibrationService.PixelGroupingParametersCalculationRecipe",
        return_value=MagicMock(executeRecipe=MagicMock(side_effect=[{"parameters": "mock_parameters"}])),
    )
    @mock.patch("snapred.backend.service.CalibrationService.PixelGroupingIngredients")
    @mock.patch("snapred.backend.service.CalibrationService.DataExportService")
    def test_calculatePixelGroupingParameters(
        mockDataFactoryService,  # noqa: ARG001
        mockPixelGroupingIngredients,  # noqa: ARG001
        mockPixelGroupingParametersCalculationRecipe,
        mockDataExportService,  # noqa: ARG001
    ):
        runs = [RunConfig(runNumber="1")]
        calibrationService = CalibrationService()
        groupingFile = mock.Mock()
        setattr(PixelGroupingParametersCalculationRecipe, "executeRecipe", localMock)
        localMock.return_value = mock.MagicMock()
        calibrationService.calculatePixelGroupingParameters(runs, groupingFile)
        assert mockPixelGroupingParametersCalculationRecipe.called


from snapred.backend.service.CalibrationService import CalibrationService  # noqa: E402, F811


class TestCalibrationServiceMethods(unittest.TestCase):
    def setUp(self):
        self.instance = CalibrationService()
        self.runId = "test_run_id"
        self.outputNameFormat = "{}_calibration_reduction_result"
        # Mock the _calculatePixelGroupingParameters method to return a predefined value
        self.instance._calculatePixelGroupingParameters = lambda calib, focus_def: {  # noqa: ARG005
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
        with self.assertRaises(Exception) as context:
            self.instance._loadFocusedData(self.runId)

        # Assert the Exception message is as expected
        expected_message = "No focussed data found for run {}, Please run Calibration Reduction on this Data.".format(
            self.runId
        )
        assert str(context.exception) == expected_message

    def test_GetPixelGroupingParams(self):
        # Mock input data
        calibration = "calibration_data"
        focusGroups = [MagicMock(definition="focus1"), MagicMock(definition="focus2"), MagicMock(definition="focus3")]

        # Call the method to test
        result = self.instance._getPixelGroupingParams(calibration, focusGroups)

        # Assert the result is as expected
        expected_result = ["params for focus1", "params for focus2", "params for focus3"]
        assert result == expected_result

    def test_GetPixelGroupingParams_Empty(self):
        # Test with an empty focusGroups list
        calibration = "calibration_data"
        focusGroups = []

        # Call the method to test
        result = self.instance._getPixelGroupingParams(calibration, focusGroups)

        # Assert the result is an empty list
        assert result == []

    @patch("snapred.backend.service.CalibrationService.GroupWorkspaceIterator", return_value=["ws1", "ws2"])
    @patch(
        "snapred.backend.service.CalibrationService.FitCalibrationWorkspaceIngredients",
        side_effect=["fit_result_1", "fit_result_2"],
    )
    @patch(
        "snapred.backend.service.CalibrationService.FitCalibrationWorkspaceRecipe",
        return_value=MagicMock(executeRecipe=MagicMock(side_effect=["fit_result_1", "fit_result_2"])),
    )
    @patch(
        "snapred.backend.service.CalibrationService.CalibrationMetricExtractionRecipe",
        return_value=MagicMock(executeRecipe=MagicMock("dsfdfad")),
    )
    @patch("snapred.backend.service.CalibrationService.list_to_raw", return_value="mock_metric")
    @patch("snapred.backend.service.CalibrationService.parse_raw_as", return_value="mock_crystal_info")
    @patch("snapred.backend.service.CalibrationService.FocusGroupMetric", return_value="mock_metric")
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
        # Mock input data
        mockInstrumentState = MagicMock()
        fakeFocussedData = "mock_focussed_data"
        fakeFocusGroups = [MagicMock(name="group1"), MagicMock(name="group2")]
        fakePixelGroupingParams = [{"param1": 1}, {"param2": 2}]
        fakeCrystalInfo = "mock_crystal_info"

        # Mock the necessary method calls
        mockGroupWorkspaceIterator = MagicMock(return_value=["ws1", "ws2"])
        # FitCalibrationWorkspaceRecipe = MagicMock(side_effect=["fit_result_1", "fit_result_2"])
        # fitCalibrationWorkspaceRecipe.executeRecipe = MagicMock(return_value="fit_result")
        mockCalibrationMetricExtractionRecipe = MagicMock(return_value="mock_metric")
        self.instance.GroupWorkspaceIterator = mockGroupWorkspaceIterator
        self.instance.CalibrationMetricExtractionRecipe = mockCalibrationMetricExtractionRecipe

        # Call the method to test
        fittedWorkspaceNames, metrics = self.instance._fitAndCollectMetrics(
            mockInstrumentState, fakeFocussedData, fakeFocusGroups, fakePixelGroupingParams, fakeCrystalInfo
        )

        # Assert the results are as expected
        expected_fitted_workspace_names = ["fit_result_1", "fit_result_2"]
        assert fittedWorkspaceNames == expected_fitted_workspace_names

        expected_metrics = ["mock_metric", "mock_metric"]
        assert metrics == expected_metrics

    @patch(
        "snapred.backend.service.CalibrationService.CrystallographicInfoService",
        return_value=MagicMock(ingest=MagicMock(return_value={"crystalInfo": "mock_crystal_info"})),
    )
    @patch("snapred.backend.service.CalibrationService.CalibrationRecord", return_value="mock_calibration_record")
    def test_assessQuality(self, mockCrystallographicInfoService, mockCalibrationRecord):  # noqa: ARG002
        # Mock input data
        mockRequest = MagicMock()
        mockRun = MagicMock()
        fakeCifPath = "mock_cif_path"
        mockReductionIngredients = MagicMock()
        mockCalibration = MagicMock()
        mockInstrumentState = MagicMock()
        fakeFocussedData = "mock_focussed_data"
        fakePixelGroupingParams = [{"param1": 1}, {"param2": 2}]
        fakeFittedWorkspaceNames = ["ws1", "ws2"]
        fakeMetrics = [
            {"focusGroupName": "focus1", "calibrationMetric": "metric1"},
            {"focusGroupName": "focus2", "calibrationMetric": "metric2"},
        ]
        fakeFocusGroupParameters = {"focus1": {"param1": 1}, "focus2": {"param2": 2}}

        # Mock the necessary method calls
        mockRequest.run = mockRun
        mockRequest.cifPath = fakeCifPath
        self.instance.dataFactoryService.getReductionIngredients = MagicMock(return_value=mockReductionIngredients)
        self.instance.dataFactoryService.getCalibrationState = MagicMock(return_value=mockCalibration)
        mockCalibration.instrumentState = mockInstrumentState
        self.instance._loadFocusedData = MagicMock(return_value=fakeFocussedData)
        self.instance._getPixelGroupingParams = MagicMock(return_value=fakePixelGroupingParams)
        self.instance._fitAndCollectMetrics = MagicMock(return_value=(fakeFittedWorkspaceNames, fakeMetrics))
        self.instance.collectFocusGroupParameters = MagicMock(return_value=fakeFocusGroupParameters)

        # Call the method to test
        result = self.instance.assessQuality(mockRequest)

        # Assert the result is as expected
        expected_record = "mock_calibration_record"
        assert result == expected_record
