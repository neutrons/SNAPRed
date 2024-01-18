import unittest.mock as mock
from unittest.mock import MagicMock

import pytest
from snapred.backend.dao.ingredients import ReductionIngredients
from snapred.backend.dao.InstrumentConfig import InstrumentConfig
from snapred.backend.dao.Limit import BinnedValue, Limit
from snapred.backend.dao.ReductionState import ReductionState
from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.dao.state.PixelGroup import PixelGroup
from snapred.backend.dao.StateConfig import StateConfig

# Mock out of scope modules before importing DataFactoryService
# mock.patch("snapred.backend.data"] = mock.Mock()
with mock.patch.dict(
    "sys.modules",
    {
        "snapred.backend.data.LocalDataService": mock.Mock(),
        "snapred.backend.log": mock.Mock(),
        "snapred.backend.log.logger": mock.Mock(),
    },
):
    from snapred.backend.data.DataFactoryService import DataFactoryService

    def test_getReductionIngredients():
        dataExportService = DataFactoryService()
        dataExportService.getReductionState = mock.Mock()
        dataExportService.getReductionState.return_value = ReductionState.construct()
        dataExportService.getRunConfig = mock.Mock()
        dataExportService.getRunConfig.return_value = RunConfig.construct()

        tof = BinnedValue(
            minimum=1,
            maximum=100,
            binWidth=1,
            binningMode=1,  # LINEAR
        )
        pixelGroup = PixelGroup(
            groupIDs=[0],
            twoTheta=[0],
            dResolution=[Limit(minimum=0, maximum=0)],
            dRelativeResolution=[0],
            timeOfFlight=tof,
        )
        actual = dataExportService.getReductionIngredients(mock.Mock(), pixelGroup)

        assert type(actual) == ReductionIngredients

    def test_getReductionIngredients_noCalibration_noPixelGroup():
        dataExportService = DataFactoryService()
        dataExportService.getReductionState = mock.Mock()
        dataExportService.getReductionState.return_value = ReductionState.construct()
        dataExportService.getRunConfig = mock.Mock()
        dataExportService.getRunConfig.return_value = RunConfig.construct()
        dataExportService.getCalibrationState = mock.Mock()
        dataExportService.getCalibrationState.return_value = None
        dataExportService.constructStateId = mock.Mock()
        dataExportService.constructStateId.return_value = "expected"

        # assert exception is thrown
        pytest.raises(RuntimeError, dataExportService.getReductionIngredients, mock.Mock())

    def test_getReductionIngredients_noCalibration_pixelGroup():
        dataExportService = DataFactoryService()
        dataExportService.getReductionState = mock.Mock()
        dataExportService.getReductionState.return_value = ReductionState.construct()
        dataExportService.getRunConfig = mock.Mock()
        dataExportService.getRunConfig.return_value = RunConfig.construct()
        dataExportService.getCalibrationState = mock.Mock()
        dataExportService.getCalibrationState.return_value = None
        dataExportService.constructStateId = mock.Mock()
        dataExportService.constructStateId.return_value = "expected"

        pixelGroup = PixelGroup(
            groupIDs=[0],
            twoTheta=[0],
            dResolution=[Limit(minimum=0, maximum=0)],
            dRelativeResolution=[0],
            timeOfFlight={"minimum": 1, "maximum": 3, "binWidth": 1, "binningMode": 1},
        )
        actual = dataExportService.getReductionIngredients(mock.Mock(), pixelGroup)

        assert type(actual) == ReductionIngredients
        assert actual.pixelGroup == pixelGroup

    def test_getReductionIngredients_calibration_noPixelGroup():
        dataExportService = DataFactoryService()
        dataExportService.getReductionState = mock.Mock()
        dataExportService.getReductionState.return_value = ReductionState.construct()
        dataExportService.getRunConfig = mock.Mock()
        dataExportService.getRunConfig.return_value = RunConfig.construct()
        mockCalibration = MagicMock()
        mockInstrumentState = MagicMock()
        mockCalibration.instrumentState = mockInstrumentState
        pixelGroup = PixelGroup(
            groupIDs=[0],
            twoTheta=[0],
            dResolution=[Limit(minimum=0, maximum=0)],
            dRelativeResolution=[0],
            timeOfFlight={"minimum": 1, "maximum": 3, "binWidth": 1, "binningMode": 1},
        )
        mockInstrumentState.pixelGroup = pixelGroup
        dataExportService.getCalibrationState = mockCalibration

        dataExportService.getCalibrationState.return_value = mockCalibration
        dataExportService.constructStateId = mock.Mock()
        dataExportService.constructStateId.return_value = "expected"

        actual = dataExportService.getReductionIngredients(mock.Mock())

        assert type(actual) == ReductionIngredients
        assert actual.pixelGroup == pixelGroup

    def test_getReductionState():
        dataExportService = DataFactoryService()
        dataExportService.getInstrumentConfig = mock.Mock()
        dataExportService.getInstrumentConfig.return_value = InstrumentConfig.construct({})
        dataExportService.getStateConfig = mock.Mock()
        dataExportService.getStateConfig.return_value = StateConfig.construct({})
        actual = dataExportService.getReductionState(mock.Mock())

        assert type(actual) == ReductionState

    def test_getRunConfig():
        dataExportService = DataFactoryService()
        dataExportService.lookupService.readRunConfig = mock.Mock()
        dataExportService.lookupService.readRunConfig.return_value = RunConfig.construct()
        actual = dataExportService.getRunConfig(mock.Mock())

        assert type(actual) == RunConfig

    def test_getStateConfig():
        dataExportService = DataFactoryService()
        dataExportService.lookupService.readStateConfig = mock.Mock()
        dataExportService.lookupService.readStateConfig.return_value = StateConfig.construct()
        actual = dataExportService.getStateConfig(mock.Mock())

        assert type(actual) == StateConfig

    def test_constructStateId():
        dataExportService = DataFactoryService()
        dataExportService.lookupService_generateStateId = mock.Mock()
        dataExportService.lookupService._generateStateId.return_value = "expected"
        actual = dataExportService.constructStateId(mock.Mock())

        assert actual == "expected"

    def test_getCalibrationState():
        dataExportService = DataFactoryService()
        dataExportService.lookupService.readCalibrationState = mock.Mock()
        dataExportService.lookupService.readCalibrationState.return_value = "expected"
        actual = dataExportService.getCalibrationState(mock.Mock())

        assert actual == "expected"

    def test_getSamplePaths():
        dataExportService = DataFactoryService()
        dataExportService.lookupService.readSamplePaths = mock.Mock()
        dataExportService.lookupService.readSamplePaths.return_value = "expected"
        actual = dataExportService.getSamplePaths()

        assert actual == "expected"

    def test_getGroupingFile():
        dataExportService = DataFactoryService()
        dataExportService.lookupService.readGroupingFiles = mock.Mock()
        dataExportService.lookupService.readGroupingFiles.return_value = "expected"
        actual = dataExportService.getGroupingFiles()

        assert actual == "expected"

    def test_getCalibrantSample():
        dataExportService = DataFactoryService()
        dataExportService.lookupService.readCalibrantSample = mock.Mock()
        dataExportService.lookupService.readCalibrantSample.return_value = "expected"
        actual = dataExportService.getCalibrantSample("testId")

        assert actual == "expected"

    def test_getCifFilePath():
        dataExportService = DataFactoryService()
        dataExportService.lookupService.readCifFilePath = mock.Mock()
        dataExportService.lookupService.readCifFilePath.return_value = "expected"
        actual = dataExportService.getCifFilePath("testId")

        assert actual == "expected"

    def test_getNormalizationState():
        dataExportService = DataFactoryService()
        dataExportService.lookupService.readNormalizationState = mock.Mock()
        dataExportService.lookupService.readNormalizationState.return_value = "expected"
        actual = dataExportService.getNormalizationState(mock.Mock())

        assert actual == "expected"

    def test_getCalibrationIndex():
        dataExportService = DataFactoryService()
        dataExportService.lookupService.readCalibrationIndex = mock.Mock(return_value="expected")
        run = MagicMock()
        actual = dataExportService.getCalibrationIndex(run)

        assert actual == "expected"

    def test_getCalibrationDataPath():
        dataExportService = DataFactoryService()
        dataExportService.lookupService._constructCalibrationDataPath = mock.Mock(return_value="expected")
        actual = dataExportService.getCalibrationDataPath(mock.Mock(), mock.Mock())

        assert actual == "expected"
