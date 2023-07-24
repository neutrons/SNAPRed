import json
import os
import shutil
import unittest.mock as mock
from typing import List

import pytest
from pydantic import parse_raw_as
from snapred.meta.Config import Resource

# Mock out of scope modules before importing DataExportService
with mock.patch.dict("sys.modules", {"mantid.api": mock.Mock(), "h5py": mock.Mock()}):
    from snapred.backend.dao.calibration.Calibration import Calibration  # noqa: E402
    from snapred.backend.dao.calibration.CalibrationIndexEntry import CalibrationIndexEntry  # noqa: E402
    from snapred.backend.dao.calibration.CalibrationRecord import CalibrationRecord  # noqa: E402
    from snapred.backend.dao.ReductionIngredients import ReductionIngredients  # noqa: E402
    from snapred.backend.data.LocalDataService import LocalDataService  # noqa: E402

    reductionIngredients = None
    with Resource.open("inputs/calibration/input.json", "r") as file:
        reductionIngredients = parse_raw_as(ReductionIngredients, file.read())

    def _readInstrumentParameters():
        instrumentParmaeters = None
        with Resource.open("inputs/SNAPInstPrm.json", "r") as file:
            instrumentParmaeters = json.loads(file.read())
        return instrumentParmaeters

    def _readReductionParameters(runId: str):  # noqa: ARG001
        reductionParameters = None
        with Resource.open("inputs/ReductionParameters.json", "r") as file:
            reductionParameters = json.loads(file.read())
        reductionParameters["stateId"] = "123"
        return reductionParameters

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
        instrumentConfig.calibrationDirectory = "test"
        instrumentConfig.sharedDirectory = "test"
        instrumentConfig.reducedDataDirectory = "test"
        instrumentConfig.pixelGroupingDirectory = "test"
        return instrumentConfig

    def test_readStateConfig():
        localDataService = LocalDataService()
        localDataService._readReductionParameters = _readReductionParameters
        localDataService._readDiffractionCalibrant = mock.Mock()
        localDataService._readDiffractionCalibrant.return_value = (
            reductionIngredients.reductionState.stateConfig.diffractionCalibrant
        )
        localDataService._readNormalizationCalibrant = mock.Mock()
        localDataService._readNormalizationCalibrant.return_value = (
            reductionIngredients.reductionState.stateConfig.normalizationCalibrant
        )
        localDataService._readFocusGroups = mock.Mock()
        localDataService._readFocusGroups.return_value = []
        localDataService.instrumentConfig = getMockInstrumentConfig()

        actual = localDataService.readStateConfig(mock.Mock())
        assert actual is not None
        assert actual.stateId == "123"

    def test_readDiffractionCalibrant():
        localDataService = LocalDataService()
        localDataService._readReductionParameters = _readReductionParameters
        reductionParameters = _readReductionParameters("test")
        localDataService.instrumentConfig = getMockInstrumentConfig()
        actual = localDataService._readDiffractionCalibrant(mock.Mock())
        assert actual is not None
        assert actual.filename == reductionParameters["calFileName"]

    def test_readNormalizationCalibrant():
        localDataService = LocalDataService()
        localDataService._readReductionParameters = _readReductionParameters
        reductionParameters = _readReductionParameters("test")
        localDataService.instrumentConfig = getMockInstrumentConfig()
        actual = localDataService._readNormalizationCalibrant(mock.Mock())
        assert actual is not None
        assert actual.mask == reductionParameters["VMsk"]

    def test_readFocusGroups():
        localDataService = LocalDataService()
        localDataService._readReductionParameters = _readReductionParameters
        _readReductionParameters("test")
        localDataService.instrumentConfig = getMockInstrumentConfig()
        actual = localDataService._readFocusGroups(mock.Mock())
        assert actual is not None
        assert len(actual) == 3

    def test_readRunConfig():
        localDataService = LocalDataService()
        localDataService._readRunConfig = mock.Mock()
        localDataService._readRunConfig.return_value = reductionIngredients.runConfig
        actual = localDataService.readRunConfig(mock.Mock())
        assert actual is not None
        assert actual.runNumber == "57514"

    def test__readRunConfig():
        localDataService = LocalDataService()
        localDataService._findIPTS = mock.Mock()
        localDataService._findIPTS.return_value = "IPTS-123"
        localDataService._readReductionParameters = _readReductionParameters
        localDataService.instrumentConfig = getMockInstrumentConfig()
        actual = localDataService._readRunConfig("57514")
        assert actual is not None
        assert actual.runNumber == "57514"

    def test__findIPTS():
        localDataService = LocalDataService()
        localDataService.instrumentConfig = getMockInstrumentConfig()
        assert localDataService._findIPTS("57514") is not None

    def test_readPVFile():
        localDataService = LocalDataService()
        localDataService._readReductionParameters = _readReductionParameters
        localDataService.instrumentConfig = getMockInstrumentConfig()
        localDataService._constructPVFilePath = mock.Mock()
        localDataService._constructPVFilePath.return_value = Resource.getPath("./")
        actual = localDataService._readPVFile(mock.Mock())
        assert actual is not None

    def test__generateStateId():
        localDataService = LocalDataService()
        localDataService._readReductionParameters = _readReductionParameters
        localDataService._readPVFile = mock.Mock()
        fileMock = mock.Mock()
        localDataService._readPVFile.return_value = fileMock
        fileMock.get.side_effect = [[0.1], [0.1], [0.1], [0.1], [1]]
        actual, _ = localDataService._generateStateId(mock.Mock())
        assert actual == "9618b936a4419a6e"

    def test__findMatchingFileList():
        localDataService = LocalDataService()
        localDataService._readReductionParameters = _readReductionParameters
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
        localDataService._constructCalibrationPath = mock.Mock()
        localDataService._constructCalibrationPath.return_value = Resource.getPath("outputs")
        assert len(localDataService.readCalibrationIndex("123")) == 0

    def test_writeCalibrationIndexEntry():
        localDataService = LocalDataService()
        localDataService.instrumentConfig = mock.Mock()
        localDataService._generateStateId = mock.Mock()
        localDataService._generateStateId.return_value = ("123", "456")
        localDataService._readReductionParameters = mock.Mock()
        localDataService._constructCalibrationPath = mock.Mock()
        localDataService._constructCalibrationPath.return_value = Resource.getPath("outputs")
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

    def test_readCalibrationIndexExisting():
        localDataService = LocalDataService()
        localDataService.instrumentConfig = mock.Mock()
        localDataService._generateStateId = mock.Mock()
        localDataService._generateStateId.return_value = ("123", "456")
        localDataService._readReductionParameters = mock.Mock()
        localDataService._constructCalibrationPath = mock.Mock()
        localDataService._constructCalibrationPath.return_value = Resource.getPath("outputs")
        expectedFilePath = Resource.getPath("outputs") + "CalibrationIndex.json"
        localDataService.writeCalibrationIndexEntry(
            CalibrationIndexEntry(runNumber="57514", comments="test comment", author="test author")
        )
        actualEntries = localDataService.readCalibrationIndex("57514")
        os.remove(expectedFilePath)

        assert len(actualEntries) > 0
        assert actualEntries[0].runNumber == "57514"

    def readReductionIngredientsFromFile():
        with Resource.open("/inputs/calibration/input.json", "r") as f:
            return ReductionIngredients.parse_raw(f.read())

    def test_readWriteCalibrationRecord():
        localDataService = LocalDataService()
        localDataService.instrumentConfig = mock.Mock()
        localDataService._generateStateId = mock.Mock()
        localDataService._generateStateId.return_value = ("123", "456")
        localDataService._readReductionParameters = mock.Mock()
        localDataService._constructCalibrationPath = mock.Mock()
        localDataService._constructCalibrationPath.return_value = Resource.getPath("outputs/")
        localDataService.writeCalibrationRecord(CalibrationRecord(parameters=readReductionIngredientsFromFile()))
        actualRecord = localDataService.readCalibrationRecord("57514")
        # remove directory
        shutil.rmtree(Resource.getPath("outputs/57514"))

        assert actualRecord.parameters.runConfig.runNumber == "57514"

    def test_readWriteCalibrationRecordV2():
        localDataService = LocalDataService()
        localDataService.instrumentConfig = mock.Mock()
        localDataService._generateStateId = mock.Mock()
        localDataService._generateStateId.return_value = ("123", "456")
        localDataService._readReductionParameters = mock.Mock()
        localDataService._constructCalibrationPath = mock.Mock()
        localDataService._constructCalibrationPath.return_value = Resource.getPath("outputs/")
        localDataService.writeCalibrationRecord(CalibrationRecord(parameters=readReductionIngredientsFromFile()))
        localDataService.writeCalibrationRecord(CalibrationRecord(parameters=readReductionIngredientsFromFile()))
        actualRecord = localDataService.readCalibrationRecord("57514")
        shutil.rmtree(Resource.getPath("outputs/57514"))

        assert actualRecord.parameters.runConfig.runNumber == "57514"

    def test_getCalibrationRecordPath():
        localDataService = LocalDataService()
        localDataService._generateStateId = mock.Mock()
        localDataService._generateStateId.return_value = ("123", "456")
        localDataService._constructCalibrationPath = mock.Mock()
        localDataService._constructCalibrationPath.return_value = Resource.getPath("outputs/")
        actualPath = localDataService.getCalibrationRecordPath("57514", 1)
        assert actualPath == Resource.getPath("outputs/57514") + "/v_1/CalibrationRecord.json"

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
        import mantid.api

        mantid.api = mock.Mock()
        mantid.api.AlgorithmManager = mock.Mock()
        mantid.api.AlgorithmManager.create = mock.Mock()
        with mock.patch.dict("sys.modules", {"mantid.api": mantid.api}):
            from snapred.backend.data.LocalDataService import LocalDataService as LocalDataService2

            localDataService = LocalDataService2()
            localDataService._generateStateId = mock.Mock()
            localDataService._generateStateId.return_value = ("123", "456")
            localDataService._constructCalibrationPath = mock.Mock()
            localDataService._constructCalibrationPath.return_value = Resource.getPath("outputs/")

            localDataService.writeCalibrationReductionResult("123", "ws")

            assert mantid.api.AlgorithmManager.create.called

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

    def test__getCurrentCalibrationRecord():
        localDataService = LocalDataService()
        localDataService._getVersionFromCalibrationIndex = mock.Mock()
        localDataService._getVersionFromCalibrationIndex.return_value = "1"
        localDataService.readCalibrationRecord = mock.Mock()
        mockRecord = mock.Mock()
        localDataService.readCalibrationRecord.return_value = mockRecord
        actualRecord = localDataService._getCurrentCalibrationRecord("123")
        assert actualRecord == mockRecord

    def test_getCalibrationStatePath():
        localDataService = LocalDataService()
        localDataService._generateStateId = mock.Mock()
        localDataService._generateStateId.return_value = ("123", "456")
        localDataService._constructCalibrationPath = mock.Mock()
        localDataService._constructCalibrationPath.return_value = Resource.getPath("outputs/")
        actualPath = localDataService.getCalibrationStatePath("57514", 1)
        assert actualPath == Resource.getPath("outputs/57514") + "/v_1/CalibrationParameters.json"

    def test_readCalibrationState():
        localDataService = LocalDataService()
        localDataService._generateStateId = mock.Mock()
        localDataService._generateStateId.return_value = ("123", "456")
        localDataService.getCalibrationStatePath = mock.Mock()
        localDataService.getCalibrationStatePath.return_value = Resource.getPath("123/v_1/CalibrationParameters.json")
        localDataService._getLatestFile = mock.Mock()
        localDataService._getLatestFile.return_value = Resource.getPath("inputs/calibration/CalibrationParameters.json")
        localDataService._getCurrentCalibrationRecord = mock.Mock()
        localDataService._getCurrentCalibrationRecord.return_value = CalibrationRecord(
            parameters=readReductionIngredientsFromFile()
        )
        actualState = localDataService.readCalibrationState("123")
        assert actualState == Calibration.parse_file(Resource.getPath("inputs/calibration/CalibrationParameters.json"))

    def test_writeCalibrationState():
        localDataService = LocalDataService()
        localDataService._generateStateId = mock.Mock()
        localDataService._generateStateId.return_value = ("123", "456")
        localDataService._constructCalibrationPath = mock.Mock()
        localDataService._constructCalibrationPath.return_value = Resource.getPath("outputs/")
        localDataService._getCurrentCalibrationRecord = mock.Mock()
        localDataService._getCurrentCalibrationRecord.return_value = Calibration.construct({"name": "test"})
        with Resource.open("/inputs/calibration/CalibrationParameters.json", "r") as f:
            calibration = Calibration.parse_raw(f.read())
        localDataService.writeCalibrationState("123", calibration)
        assert os.path.exists(Resource.getPath("outputs/123/v_1/CalibrationParameters.json"))
        shutil.rmtree(Resource.getPath("outputs/123"))

    def test_initializeState():
        localDataService = LocalDataService()
        localDataService._readPVFile = mock.Mock()
        pvFileMock = mock.Mock()
        pvFileMock.get.side_effect = [[1], [2], [1.1], [1.2], [1], [1], [2]]
        localDataService._readPVFile.return_value = pvFileMock
        testCalibrationData = Calibration.parse_file(Resource.getPath("inputs/calibration/CalibrationParameters.json"))
        testCalibrationData.instrumentState.pixelGroupingInstrumentParameters = None
        localDataService.readInstrumentConfig = mock.Mock()
        localDataService.readInstrumentConfig.return_value = testCalibrationData.instrumentState.instrumentConfig
        localDataService.writeCalibrationState = mock.Mock()
        actual = localDataService.initializeState("123", "test")
        actual.creationDate = testCalibrationData.creationDate
        assert actual == testCalibrationData

    def test_badPaths():
        """This verifies that a broken configuration (from production) can't find all of the files"""
        # get a handle on the service
        service = LocalDataService()
        service.verifyPaths = True  # override test setting
        with pytest.raises(FileNotFoundError):
            service.readInstrumentConfig()

        service.verifyPaths = False  # put the setting back
