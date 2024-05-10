import os.path
import tempfile
import unittest.mock as mock

from snapred.backend.dao.InstrumentConfig import InstrumentConfig
from snapred.backend.dao.ReductionState import ReductionState
from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.dao.StateConfig import StateConfig

# Mock out of scope modules before importing DataFactoryService
# mock.patch("snapred.backend.data"] = mock.Mock()
with mock.patch.dict(
    "sys.modules",
    {
        "snapred.backend.log": mock.Mock(),
        "snapred.backend.log.logger": mock.Mock(),
    },
):
    from snapred.backend.data.DataFactoryService import DataFactoryService

    def test_fileExists_yes():
        # create a temp file that exists, and verify it exists
        with tempfile.NamedTemporaryFile(suffix=".biscuit") as existent:
            assert DataFactoryService().fileExists(existent.name)

    def test_fileExists_no():
        # assert that a file that does not exist, does not exist
        with tempfile.TemporaryDirectory() as tmpdir:
            nonexistent = tmpdir + "/0x0f.biscuit"
            assert not os.path.isfile(nonexistent)
            assert not DataFactoryService().fileExists(nonexistent)

    def test_getReductionState():
        dataExportService = DataFactoryService()
        dataExportService.getInstrumentConfig = mock.Mock()
        dataExportService.getInstrumentConfig.return_value = InstrumentConfig.construct({})
        dataExportService.getStateConfig = mock.Mock()
        dataExportService.getStateConfig.return_value = StateConfig.construct({})
        actual = dataExportService.getReductionState("123", False)

        assert type(actual) == ReductionState

    def test_getRunConfig():
        dataExportService = DataFactoryService()
        dataExportService.lookupService.readRunConfig = mock.Mock(return_value=RunConfig.construct())
        actual = dataExportService.getRunConfig(mock.Mock())

        assert type(actual) == RunConfig

    def test_getStateConfig():
        dataExportService = DataFactoryService()
        dataExportService.lookupService.readStateConfig = mock.Mock(return_value=StateConfig.construct())
        actual = dataExportService.getStateConfig(mock.Mock(), mock.Mock())

        assert type(actual) == StateConfig

    def test_constructStateId():
        dataExportService = DataFactoryService()
        dataExportService.lookupService._generateStateId = mock.Mock(return_value="expected")
        actual = dataExportService.constructStateId(mock.Mock())

        assert actual == "expected"

    def test_getCalibrationState():
        dataExportService = DataFactoryService()
        dataExportService.lookupService.readCalibrationState = mock.Mock(return_value="expected")
        actual = dataExportService.getCalibrationState("123", False)

        assert actual == "expected"

    def test_getGroupingMap():
        dataExportService = DataFactoryService()
        dataExportService.lookupService.readGroupingMap = mock.Mock(return_value="expected")
        actual = dataExportService.getGroupingMap(mock.Mock())
        assert actual == "expected"

    def test_checkCalibrationStateExists():
        dataExportService = DataFactoryService()
        dataExportService.lookupService.checkCalibrationFileExists = mock.Mock(return_value="expected")
        actual = dataExportService.checkCalibrationStateExists(mock.Mock())
        assert actual == "expected"

    def test_getSamplePaths():
        dataExportService = DataFactoryService()
        dataExportService.lookupService.readSamplePaths = mock.Mock(return_value="expected")
        actual = dataExportService.getSamplePaths()

        assert actual == "expected"

    def test_getCalibrantSample():
        dataExportService = DataFactoryService()
        dataExportService.lookupService.readCalibrantSample = mock.Mock(return_value="expected")
        actual = dataExportService.getCalibrantSample("testId")

        assert actual == "expected"

    def test_getCifFilePath():
        dataExportService = DataFactoryService()
        dataExportService.lookupService.readCifFilePath = mock.Mock(return_value="expected")
        actual = dataExportService.getCifFilePath("testId")

        assert actual == "expected"

    def test_getNormalizationState():
        dataExportService = DataFactoryService()
        dataExportService.lookupService.readNormalizationState = mock.Mock(return_value="expected")
        actual = dataExportService.getNormalizationState("123", False)

        assert actual == "expected"

    def test_getCalibrationIndex():
        dataExportService = DataFactoryService()
        dataExportService.lookupService.readCalibrationIndex = mock.Mock(return_value="expected")
        actual = dataExportService.getCalibrationIndex("123", False)

        assert actual == "expected"

    def test_getCalibrationDataPath():
        dataExportService = DataFactoryService()
        dataExportService.lookupService._constructCalibrationDataPath = mock.Mock(return_value="expected")
        actual = dataExportService.getCalibrationDataPath("123", True, 7)

        assert actual == "expected"

    def test_getCalibrationRecord():
        dataExportService = DataFactoryService()
        dataExportService.lookupService.readCalibrationRecord = mock.Mock(return_value="expected")
        actual = dataExportService.getCalibrationRecord(mock.Mock(), False)

        assert actual == "expected"
