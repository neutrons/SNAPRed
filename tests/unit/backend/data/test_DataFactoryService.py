import unittest.mock as mock
from unittest.mock import MagicMock

from snapred.backend.dao.ingredients import ReductionIngredients
from snapred.backend.dao.InstrumentConfig import InstrumentConfig
from snapred.backend.dao.Limit import Limit
from snapred.backend.dao.ReductionState import ReductionState
from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.dao.state import PixelGroupingParameters
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

        pixelGroupingParameters = list(MagicMock())
        actual = dataExportService.getReductionIngredients(mock.Mock(), pixelGroupingParameters)

        assert type(actual) == ReductionIngredients

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
