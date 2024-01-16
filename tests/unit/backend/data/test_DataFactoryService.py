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

    def test_getFocusGroups():
        dataExportService = DataFactoryService()
        dataExportService.lookupService.readFocusGroups = mock.Mock()
        dataExportService.lookupService.readFocusGroups.return_value = "expected"
        actual = dataExportService.getFocusGroups()

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

    def test_getCalibrationRecord():
        dataExportService = DataFactoryService()
        dataExportService.lookupService.readCalibrationRecord = mock.Mock(return_value="expected")
        actual = dataExportService.getCalibrationRecord(mock.Mock(), mock.Mock())

        assert actual == "expected"
