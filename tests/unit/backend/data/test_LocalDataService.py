import json
import os
import shutil
import socket
import tempfile
import unittest.mock as mock
from pathlib import Path
from typing import List

import pytest
from pydantic import parse_raw_as
from snapred.backend.dao.state.CalibrantSample.CalibrantSamples import CalibrantSamples

# NOTE this is necessary to prevent mocking out needed functions
from snapred.backend.recipe.algorithm.WashDishes import WashDishes
from snapred.meta.Config import Config, Resource

IS_ON_ANALYSIS_MACHINE = socket.gethostname().startswith("analysis")

# Mock out of scope modules before importing DataExportService
with mock.patch.dict("sys.modules", {"mantid.api": mock.Mock(), "h5py": mock.Mock()}):
    from snapred.backend.dao.calibration.Calibration import Calibration  # noqa: E402
    from snapred.backend.dao.calibration.CalibrationIndexEntry import CalibrationIndexEntry  # noqa: E402
    from snapred.backend.dao.calibration.CalibrationRecord import CalibrationRecord  # noqa: E402
    from snapred.backend.dao.ingredients import ReductionIngredients  # noqa: E402
    from snapred.backend.dao.normalization.Normalization import Normalization  # noqa: E402
    from snapred.backend.dao.normalization.NormalizationIndexEntry import NormalizationIndexEntry  # noqa: E402
    from snapred.backend.dao.normalization.NormalizationRecord import NormalizationRecord  # noqa: E402
    from snapred.backend.dao.state.FocusGroup import FocusGroup
    from snapred.backend.data.LocalDataService import LocalDataService  # noqa: E402

    reductionIngredients = None
    with Resource.open("inputs/calibration/input.json", "r") as file:
        reductionIngredients = parse_raw_as(ReductionIngredients, file.read())

    def _readInstrumentParameters():
        instrumentParmaeters = None
        with Resource.open("inputs/SNAPInstPrm.json", "r") as file:
            instrumentParmaeters = json.loads(file.read())
        return instrumentParmaeters

    def test_readInstrumentConfig():
        localDataService = LocalDataService()
        localDataService._readInstrumentParameters = _readInstrumentParameters
        actual = localDataService.readInstrumentConfig()
        assert actual is not None
        assert actual.version == "1.4"
        assert actual.name == "SNAP"

    def test_readInstrumentParameters():
        localDataService = LocalDataService()
        localDataService.instrumentConfigPath = Resource.getPath("inputs/SNAPInstPrm.json")
        actual = localDataService._readInstrumentParameters()
        assert actual is not None
        assert actual["version"] == 1.4
        assert actual["name"] == "SNAP"

    def getMockInstrumentConfig():
        instrumentConfig = mock.Mock()
        instrumentConfig.calibrationDirectory = Path("test")
        instrumentConfig.sharedDirectory = "test"
        instrumentConfig.reducedDataDirectory = "test"
        instrumentConfig.pixelGroupingDirectory = "test"
        instrumentConfig.delTOverT = 1
        instrumentConfig.nexusDirectory = "test"
        instrumentConfig.nexusFileExtension = "test"
        return instrumentConfig

    def test_readStateConfig():
        localDataService = LocalDataService()
        localDataService._readDiffractionCalibrant = mock.Mock()
        localDataService._readDiffractionCalibrant.return_value = (
            reductionIngredients.reductionState.stateConfig.diffractionCalibrant
        )
        localDataService._readNormalizationCalibrant = mock.Mock()
        localDataService._readNormalizationCalibrant.return_value = (
            reductionIngredients.reductionState.stateConfig.normalizationCalibrant
        )
        localDataService.groceryService.getIPTS = mock.Mock(return_value="IPTS-123")
        localDataService._readPVFile = mock.Mock()
        fileMock = mock.Mock()
        localDataService._readPVFile.return_value = fileMock
        localDataService._generateStateId = mock.Mock()
        localDataService._generateStateId.return_value = ("123", "456")
        localDataService.readCalibrationState = mock.Mock()
        localDataService.readCalibrationState.return_value = Calibration.parse_file(
            Resource.getPath("inputs/calibration/CalibrationParameters.json")
        )
        localDataService.instrumentConfig = getMockInstrumentConfig()

        actual = localDataService.readStateConfig("123")
        assert actual is not None
        assert actual.stateId == "123"

    def test_readRunConfig():
        localDataService = LocalDataService()
        localDataService._readRunConfig = mock.Mock()
        localDataService._readRunConfig.return_value = reductionIngredients.runConfig
        actual = localDataService.readRunConfig(mock.Mock())
        assert actual is not None
        assert actual.runNumber == "57514"

    def test__readRunConfig():
        localDataService = LocalDataService()
        localDataService.groceryService.getIPTS = mock.Mock(return_value="IPTS-123")
        localDataService.instrumentConfig = getMockInstrumentConfig()
        actual = localDataService._readRunConfig("57514")
        assert actual is not None
        assert actual.runNumber == "57514"

    def test_readPVFile():
        localDataService = LocalDataService()
        localDataService.instrumentConfig = getMockInstrumentConfig()
        localDataService._constructPVFilePath = mock.Mock()
        localDataService._constructPVFilePath.return_value = Resource.getPath("./")
        actual = localDataService._readPVFile(mock.Mock())
        assert actual is not None

    def test__generateStateId():
        localDataService = LocalDataService()
        localDataService._readPVFile = mock.Mock()
        fileMock = mock.Mock()
        localDataService._readPVFile.return_value = fileMock
        fileMock.get.side_effect = [[0.1], [0.1], [0.1], [0.1], [1]]
        actual, _ = localDataService._generateStateId(mock.Mock())
        assert actual == "9618b936a4419a6e"

    def test__findMatchingFileList():
        localDataService = LocalDataService()
        localDataService.instrumentConfig = getMockInstrumentConfig()
        actual = localDataService._findMatchingFileList(Resource.getPath("inputs/SNAPInstPrm.json"), False)
        assert actual is not None
        assert len(actual) == 1

    def test_readCalibrationIndexMissing():
        localDataService = LocalDataService()
        localDataService.instrumentConfig = mock.Mock()
        localDataService._generateStateId = mock.Mock()
        localDataService._generateStateId.return_value = ("123", "456")
        localDataService._readReductionParameters = mock.Mock()
        localDataService._constructCalibrationStatePath = mock.Mock()
        localDataService._constructCalibrationStatePath.return_value = Resource.getPath("outputs")
        assert len(localDataService.readCalibrationIndex("123")) == 0

    def test_readNormalizationIndexMissing():
        localDataService = LocalDataService()
        localDataService.instrumentConfig = mock.Mock()
        localDataService._generateStateId = mock.Mock()
        localDataService._generateStateId.return_value = ("123", "456")
        localDataService._readReductionParameters = mock.Mock()
        localDataService._constructCalibrationStatePath = mock.Mock()
        localDataService._constructCalibrationStatePath.return_value = Resource.getPath("outputs")
        assert len(localDataService.readNormalizationIndex("123")) == 0

    def test_writeCalibrationIndexEntry():
        localDataService = LocalDataService()
        localDataService.instrumentConfig = mock.Mock()
        localDataService._generateStateId = mock.Mock()
        localDataService._generateStateId.return_value = ("123", "456")
        localDataService._readReductionParameters = mock.Mock()
        localDataService._constructCalibrationStatePath = mock.Mock()
        localDataService._constructCalibrationStatePath.return_value = Resource.getPath("outputs")
        expectedFilePath = Resource.getPath("outputs") + "CalibrationIndex.json"
        localDataService.writeCalibrationIndexEntry(
            CalibrationIndexEntry(runNumber="57514", comments="test comment", author="test author")
        )
        assert os.path.exists(expectedFilePath)

        fileContent = ""
        with open(expectedFilePath, "r") as indexFile:
            fileContent = indexFile.read()
        os.remove(expectedFilePath)
        assert len(fileContent) > 0

        actualEntries = parse_raw_as(List[CalibrationIndexEntry], fileContent)
        assert len(actualEntries) > 0
        assert actualEntries[0].runNumber == "57514"

        # def test_writeNormalizationIndexEntry():
        localDataService = LocalDataService()
        localDataService.instrumentConfig = mock.Mock()
        localDataService._generateStateId = mock.Mock()
        localDataService._generateStateId.return_value = ("123", "456")
        localDataService._readReductionParameters = mock.Mock()
        localDataService._constructCalibrationStatePath = mock.Mock()
        localDataService._constructCalibrationStatePath.return_value = Resource.getPath("outputs")
        expectedFilePath = Resource.getPath("outputs") + "NormalizationIndex.json"
        localDataService.writeNormalizationIndexEntry(
            NormalizationIndexEntry(
                runNumber="57514", backgroundRunNumber="58813", comments="test comment", author="test author"
            )
        )
        assert os.path.exists(expectedFilePath)

        fileContent = ""
        with open(expectedFilePath, "r") as indexFile:
            fileContent = indexFile.read()
        os.remove(expectedFilePath)
        assert len(fileContent) > 0

        actualEntries = parse_raw_as(List[NormalizationIndexEntry], fileContent)
        assert len(actualEntries) > 0
        assert actualEntries[0].runNumber == "57514"

    def test_readCalibrationIndexExisting():
        localDataService = LocalDataService()
        localDataService.instrumentConfig = mock.Mock()
        localDataService._generateStateId = mock.Mock()
        localDataService._generateStateId.return_value = ("123", "456")
        localDataService._readReductionParameters = mock.Mock()
        localDataService._constructCalibrationStatePath = mock.Mock()
        localDataService._constructCalibrationStatePath.return_value = Resource.getPath("outputs")
        expectedFilePath = Resource.getPath("outputs") + "CalibrationIndex.json"
        localDataService.writeCalibrationIndexEntry(
            CalibrationIndexEntry(runNumber="57514", comments="test comment", author="test author")
        )
        actualEntries = localDataService.readCalibrationIndex("57514")
        os.remove(expectedFilePath)

        assert len(actualEntries) > 0
        assert actualEntries[0].runNumber == "57514"

    def test_readNormalizationIndexExisting():
        localDataService = LocalDataService()
        localDataService.instrumentConfig = mock.Mock()
        localDataService._generateStateId = mock.Mock()
        localDataService._generateStateId.return_value = ("123", "456")
        localDataService._readReductionParameters = mock.Mock()
        localDataService._constructCalibrationStatePath = mock.Mock()
        localDataService._constructCalibrationStatePath.return_value = Resource.getPath("outputs")
        expectedFilePath = Resource.getPath("outputs") + "NormalizationIndex.json"
        localDataService.writeNormalizationIndexEntry(
            NormalizationIndexEntry(
                runNumber="57514", backgroundRunNumber="58813", comments="test comment", author="test author"
            )
        )
        actualEntries = localDataService.readNormalizationIndex("57514")
        os.remove(expectedFilePath)

        assert len(actualEntries) > 0
        assert actualEntries[0].runNumber == "57514"

    def readReductionIngredientsFromFile():
        with Resource.open("/inputs/calibration/input.json", "r") as f:
            return ReductionIngredients.parse_raw(f.read())

    def test_readWriteCalibrationRecord():
        with tempfile.TemporaryDirectory(prefix=Resource.getPath("outputs/")) as tempdir:
            localDataService = LocalDataService()
            localDataService.instrumentConfig = mock.Mock()
            localDataService._generateStateId = mock.Mock()
            localDataService._generateStateId.return_value = ("123", "456")
            localDataService._readReductionParameters = mock.Mock()
            localDataService._constructCalibrationStatePath = mock.Mock()
            localDataService._constructCalibrationStatePath.return_value = f"{tempdir}/"
            localDataService.groceryService = mock.Mock()
            localDataService.writeCalibrationRecord(
                CalibrationRecord.parse_raw(Resource.read("inputs/calibration/CalibrationRecord.json"))
            )
            actualRecord = localDataService.readCalibrationRecord("57514")
        assert actualRecord.runNumber == "57514"

    def test_readWriteCalibrationRecordV2():
        with tempfile.TemporaryDirectory(prefix=Resource.getPath("outputs/")) as tempdir:
            localDataService = LocalDataService()
            localDataService.instrumentConfig = mock.Mock()
            localDataService._generateStateId = mock.Mock()
            localDataService._generateStateId.return_value = ("123", "456")
            localDataService._readReductionParameters = mock.Mock()
            localDataService._constructCalibrationStatePath = mock.Mock()
            localDataService._constructCalibrationStatePath.return_value = f"{tempdir}/"
            localDataService.groceryService = mock.Mock()
            localDataService.writeCalibrationRecord(
                CalibrationRecord.parse_raw(Resource.read("inputs/calibration/CalibrationRecord.json"))
            )
            localDataService.writeCalibrationRecord(
                CalibrationRecord.parse_raw(Resource.read("inputs/calibration/CalibrationRecord.json"))
            )
            actualRecord = localDataService.readCalibrationRecord("57514")
        assert actualRecord.runNumber == "57514"

    def test_readWriteNormalizationRecord():
        with tempfile.TemporaryDirectory(prefix=Resource.getPath("outputs/")) as tempdir:
            localDataService = LocalDataService()
            localDataService.instrumentConfig = mock.Mock()
            localDataService._generateStateId = mock.Mock()
            localDataService._generateStateId.return_value = ("123", "456")
            localDataService._readReductionParameters = mock.Mock()
            localDataService._constructCalibrationStatePath = mock.Mock()
            localDataService._constructCalibrationStatePath.return_value = f"{tempdir}/"
            localDataService.groceryService = mock.Mock()
            localDataService.writeNormalizationRecord(
                NormalizationRecord.parse_raw(Resource.read("inputs/normalization/NormalizationRecord.json"))
            )
            localDataService.writeNormalizationRecord(
                NormalizationRecord.parse_raw(Resource.read("inputs/normalization/NormalizationRecord.json"))
            )
            actualRecord = localDataService.readNormalizationRecord("57514")
        assert actualRecord.runNumber == "57514"

    def test_getCalibrationRecordPath():
        localDataService = LocalDataService()
        localDataService._generateStateId = mock.Mock()
        localDataService._generateStateId.return_value = ("123", "456")
        localDataService._constructCalibrationStatePath = mock.Mock()
        localDataService._constructCalibrationStatePath.return_value = Resource.getPath("outputs/")
        actualPath = localDataService.getCalibrationRecordPath("57514", 1)
        assert actualPath == Resource.getPath("outputs") + "/v_1/CalibrationRecord.json"

    def test_getNormalizationRecordPath():
        localDataService = LocalDataService()
        localDataService._generateStateId = mock.Mock()
        localDataService._generateStateId.return_value = ("123", "456")
        localDataService._constructCalibrationStatePath = mock.Mock()
        localDataService._constructCalibrationStatePath.return_value = Resource.getPath("outputs/")
        actualPath = localDataService.getNormalizationRecordPath("57514", 1)
        assert actualPath == Resource.getPath("outputs") + "/v_1/NormalizationRecord.json"

    def test_extractFileVersion():
        localDataService = LocalDataService()
        actualVersion = localDataService._extractFileVersion("Powder/1234/v_4/CalibrationRecord.json")
        assert actualVersion == 4

    def test__getFileOfVersion():
        localDataService = LocalDataService()
        localDataService._findMatchingFileList = mock.Mock()
        localDataService._findMatchingFileList.return_value = [
            "/v_1/CalibrationRecord.json",
            "/v_3/CalibrationRecord.json",
        ]
        actualFile = localDataService._getFileOfVersion("/v_*/CalibrationRecord", 3)
        assert actualFile == "/v_3/CalibrationRecord.json"

    def test__getLatestFile():
        localDataService = LocalDataService()
        localDataService._findMatchingFileList = mock.Mock()
        localDataService._findMatchingFileList.return_value = [
            "Powder/1234/v_1/CalibrationRecord.json",
            "Powder/1234/v_2/CalibrationRecord.json",
        ]
        actualFile = localDataService._getLatestFile("Powder/1234/v_*/CalibrationRecord.json")
        assert actualFile == "Powder/1234/v_2/CalibrationRecord.json"

    def test_writeCalibrationReductionResult():
        from snapred.backend.data.LocalDataService import LocalDataService as LocalDataService2

        localDataService = LocalDataService2()
        localDataService._generateStateId = mock.Mock()
        localDataService._generateStateId.return_value = ("123", "456")
        localDataService._constructCalibrationStatePath = mock.Mock()
        localDataService._constructCalibrationStatePath.return_value = Resource.getPath("outputs/")

        filename = localDataService.writeCalibrationReductionResult("123", "ws", dryrun=True)
        assert filename.endswith("tests/resources/outputs/123/ws_v1.nxs")

    def test_writeCalibrationReductionResult_notdryrun():
        from snapred.backend.data.LocalDataService import LocalDataService as LocalDataService2

        localDataService = LocalDataService2()
        localDataService.groceryService = mock.Mock()
        localDataService._generateStateId = mock.Mock()
        localDataService._generateStateId.return_value = ("123", "456")
        localDataService._constructCalibrationStatePath = mock.Mock()
        localDataService._constructCalibrationStatePath.return_value = Resource.getPath("outputs/")

        filename = localDataService.writeCalibrationReductionResult("123", "ws", dryrun=False)
        assert filename.endswith("tests/resources/outputs/123/ws_v1.nxs")
        assert localDataService.groceryService.writeWorkspace.called_once_with(filename, "ws")

    def test__isApplicableEntry_equals():
        localDataService = LocalDataService()
        entry = mock.Mock()
        entry.appliesTo = "123"
        assert localDataService._isApplicableEntry(entry, "123")

    def test__isApplicableEntry_greaterThan():
        localDataService = LocalDataService()
        entry = mock.Mock()
        entry.appliesTo = ">123"
        assert localDataService._isApplicableEntry(entry, "456")

    def test__isApplicableEntry_lessThan():
        localDataService = LocalDataService()
        entry = mock.Mock()
        entry.appliesTo = "<123"
        assert localDataService._isApplicableEntry(entry, "99")

    def test__getVersionFromCalibrationIndex():
        localDataService = LocalDataService()
        localDataService.readCalibrationIndex = mock.Mock()
        localDataService.readCalibrationIndex.return_value = [mock.Mock()]
        localDataService.readCalibrationIndex.return_value[0] = CalibrationIndexEntry(
            timestamp=123, version=1, appliesTo="123", runNumber="123", comments="", author=""
        )
        actualVersion = localDataService._getVersionFromCalibrationIndex("123")
        assert actualVersion == "1"

    def test__getVersionFromNormalizationIndex():
        localDataService = LocalDataService()
        localDataService.readNormalizationIndex = mock.Mock()
        localDataService.readNormalizationIndex.return_value = [mock.Mock()]
        localDataService.readNormalizationIndex.return_value[0] = NormalizationIndexEntry(
            timestamp=123,
            version=1,
            appliesTo="123",
            runNumber="123",
            backgroundRunNumber="456",
            comments="",
            author="",
        )
        actualVersion = localDataService._getVersionFromNormalizationIndex("123")
        assert actualVersion == "1"

    def test__getCurrentCalibrationRecord():
        localDataService = LocalDataService()
        localDataService._getVersionFromCalibrationIndex = mock.Mock()
        localDataService._getVersionFromCalibrationIndex.return_value = "1"
        localDataService.readCalibrationRecord = mock.Mock()
        mockRecord = mock.Mock()
        localDataService.readCalibrationRecord.return_value = mockRecord
        actualRecord = localDataService._getCurrentCalibrationRecord("123")
        assert actualRecord == mockRecord

    def test__getCurrentNormalizationRecord():
        localDataService = LocalDataService()
        localDataService._getVersionFromNormalizationIndex = mock.Mock()
        localDataService._getVersionFromNormalizationIndex.return_value = "1"
        localDataService.readNormalizationRecord = mock.Mock()
        mockRecord = mock.Mock()
        localDataService.readNormalizationRecord.return_value = mockRecord
        actualRecord = localDataService._getCurrentNormalizationRecord("123")
        assert actualRecord == mockRecord

    def test_getCalibrationStatePath():
        localDataService = LocalDataService()
        localDataService._generateStateId = mock.Mock()
        localDataService._generateStateId.return_value = ("123", "456")
        localDataService._constructCalibrationStatePath = mock.Mock()
        localDataService._constructCalibrationStatePath.return_value = Resource.getPath("outputs/")
        actualPath = localDataService.getCalibrationStatePath("57514", 1)
        assert actualPath == Resource.getPath("outputs") + "/v_1/CalibrationParameters.json"

    def test_readCalibrationState():
        localDataService = LocalDataService()
        localDataService._generateStateId = mock.Mock()
        localDataService._generateStateId.return_value = ("123", "456")
        localDataService.getCalibrationStatePath = mock.Mock()
        localDataService.getCalibrationStatePath.return_value = Resource.getPath("123/v_1/CalibrationParameters.json")
        localDataService._getLatestFile = mock.Mock()
        localDataService._getLatestFile.return_value = Resource.getPath("inputs/calibration/CalibrationParameters.json")
        localDataService._getCurrentCalibrationRecord = mock.Mock()
        localDataService._getCurrentCalibrationRecord.return_value = CalibrationRecord.parse_raw(
            Resource.read("inputs/calibration/CalibrationRecord.json")
        )
        actualState = localDataService.readCalibrationState("123")
        assert actualState == Calibration.parse_file(Resource.getPath("inputs/calibration/CalibrationParameters.json"))

    def test_readNormalizationState():
        localDataService = LocalDataService()
        localDataService._generateStateId = mock.Mock()
        localDataService._generateStateId.return_value = ("123", "456")
        localDataService.getCalibrationStatePath = mock.Mock()
        localDataService.getCalibrationStatePath.return_value = Resource.getPath("123/v_1/CalibrationParameters.json")
        localDataService._getLatestFile = mock.Mock()
        localDataService._getLatestFile.return_value = Resource.getPath("inputs/calibration/CalibrationParameters.json")
        localDataService._getCurrentNormalizationRecord = mock.Mock()
        localDataService._getCurrentNormalizationRecord.return_value = NormalizationRecord.parse_raw(
            Resource.read("inputs/normalization/NormalizationRecord.json")
        )
        actualState = localDataService.readNormalizationState("123")
        expectedState = Normalization.parse_file(Resource.getPath("inputs/normalization/NormalizationParameters.json"))
        assert actualState == expectedState

    def test_writeCalibrationState():
        with tempfile.TemporaryDirectory(prefix=Resource.getPath("outputs/")) as tempdir:
            print(f"TEMP DIR = {tempdir}")
            localDataService = LocalDataService()
            localDataService._generateStateId = mock.Mock()
            localDataService._generateStateId.return_value = ("123", "456")
            localDataService._constructCalibrationStatePath = mock.Mock()
            localDataService._constructCalibrationStatePath.return_value = f"{tempdir}/"
            localDataService._getCurrentCalibrationRecord = mock.Mock()
            localDataService._getCurrentCalibrationRecord.return_value = Calibration.construct({"name": "test"})
            calibration = Calibration.parse_raw(Resource.read("/inputs/calibration/CalibrationParameters.json"))
            localDataService.writeCalibrationState("123", calibration)
            assert os.path.exists(tempdir + "/v_1/CalibrationParameters.json")

    def test_writeNormalizationState():
        with tempfile.TemporaryDirectory(prefix=Resource.getPath("outputs/")) as tempdir:
            localDataService = LocalDataService()
            localDataService._generateStateId = mock.Mock()
            localDataService._generateStateId.return_value = ("123", "456")
            localDataService._constructCalibrationStatePath = mock.Mock()
            localDataService._constructCalibrationStatePath.return_value = f"{tempdir}/"
            localDataService._getCurrentNormalizationRecord = mock.Mock()
            localDataService._getCurrentNormalizationRecord.return_value = Normalization.construct({"name": "test"})
            with Resource.open("/inputs/normalization/NormalizationParameters.json", "r") as f:
                normalization = Normalization.parse_raw(f.read())
            localDataService.writeNormalizationState("123", normalization)
            print(os.listdir(tempdir + "/v_1/"))
            assert os.path.exists(tempdir + "/v_1/NormalizationParameters.json")

    def test_initializeState():
        localDataService = LocalDataService()
        localDataService._readPVFile = mock.Mock()
        pvFileMock = mock.Mock()
        pvFileMock.get.side_effect = [[1], [2], [1.1], [1.2], [1], [1], [2]]
        localDataService._readPVFile.return_value = pvFileMock
        testCalibrationData = Calibration.parse_file(Resource.getPath("inputs/calibration/CalibrationParameters.json"))
        localDataService.readInstrumentConfig = mock.Mock()
        localDataService.readInstrumentConfig.return_value = testCalibrationData.instrumentState.instrumentConfig
        localDataService.writeCalibrationState = mock.Mock()
        actual = localDataService.initializeState("123", "test")
        actual.creationDate = testCalibrationData.creationDate
        assert actual == testCalibrationData

    # NOTE: This test fails on analysis because the instrument home actually does exist!
    @pytest.mark.skipif(
        IS_ON_ANALYSIS_MACHINE, reason="This test fails on analysis because the instrument home actually does exist!"
    )
    def test_badPaths():
        """This verifies that a broken configuration (from production) can't find all of the files"""
        # get a handle on the service
        service = LocalDataService()
        service.verifyPaths = True  # override test setting
        prevInstrumentHome = Config._config["instrument"]["home"]
        Config._config["instrument"]["home"] = "/this/path/does/not/exist"
        with pytest.raises(FileNotFoundError):
            service.readInstrumentConfig()
        Config._config["instrument"]["home"] = prevInstrumentHome
        service.verifyPaths = False  # put the setting back

    def test_readSamplePaths():
        localDataService = LocalDataService()
        localDataService._findMatchingFileList = mock.Mock()
        localDataService._findMatchingFileList.return_value = [
            "/sample1.json",
            "/sample2.json",
        ]
        result = localDataService.readSamplePaths()
        assert len(result) == 2
        assert "/sample1.json" in result
        assert "/sample2.json" in result

    def test_readNoSamplePaths():
        localDataService = LocalDataService()
        localDataService._findMatchingFileList = mock.Mock()
        localDataService._findMatchingFileList.return_value = []

        with pytest.raises(RuntimeError) as e:
            localDataService.readSamplePaths()
        assert "No samples found" in str(e.value)

    def test_readGroupingFiles():
        localDataService = LocalDataService()
        localDataService._findMatchingFileList = mock.Mock()
        localDataService._findMatchingFileList.side_effect = lambda path, throws: [  # noqa: ARG005
            f"/group1.{path.split('/')[-1].split('.')[-1]}",
            f"/group2.{path.split('/')[-1].split('.')[-1]}",
        ]
        result = localDataService.readGroupingFiles()
        # 6 because there are 3 file types and the mock returns 2 files per
        assert len(result) == 6
        assert result == [f"/group{x}.{ext}" for ext in ["xml", "nxs", "hdf"] for x in [1, 2]]

    def test_readNoGroupingFiles():
        localDataService = LocalDataService()
        localDataService._findMatchingFileList = mock.Mock()
        localDataService._findMatchingFileList.return_value = []
        try:
            localDataService.readGroupingFiles()
            pytest.fail("Should have thrown an exception")
        except RuntimeError:
            assert True

    def test_reaFocusGroups():
        localDataService = LocalDataService()
        localDataService._findMatchingFileList = mock.Mock()
        localDataService._findMatchingFileList.side_effect = lambda path, throws: [  # noqa: ARG005
            f"/group1.{path.split('/')[-1].split('.')[-1]}",
            f"/group2.{path.split('/')[-1].split('.')[-1]}",
        ]
        result = localDataService.readFocusGroups()
        # 6 because there are 3 file types and the mock returns 2 files per
        assert len(result) == 6
        assert list(result.keys()) == [f"/group{x}.{ext}" for ext in ["xml", "nxs", "hdf"] for x in [1, 2]]
        assert list(result.values()) == [
            FocusGroup(name=x.split("/")[-1].split(".")[0], definition=x) for x in list(result.keys())
        ]

    @mock.patch("os.path.exists", return_value=True)
    def test_readCalibrantSample(mock1):  # noqa: ARG001
        with mock.patch("os.path.exists", return_value=True):
            localDataService = LocalDataService()

            result = localDataService.readCalibrantSample(
                Resource.getPath("inputs/calibrantSamples/Silicon_NIST_640D_001.json")
            )
            assert type(result) == CalibrantSamples
            assert result.name == "Silicon_NIST_640D"

    @mock.patch("os.path.exists", return_value=True)
    def test_readCifFilePath(mock1):  # noqa: ARG001
        with mock.patch("os.path.exists", return_value=True):
            localDataService = LocalDataService()

            result = localDataService.readCifFilePath("testid")
            assert result == "/SNS/SNAP/shared/Calibration_dynamic/CalibrantSamples/EntryWithCollCode52054_diamond.cif"
