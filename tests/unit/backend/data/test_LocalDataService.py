import os
import shutil
import unittest.mock as mock
from typing import List

from pydantic import parse_raw_as
from snapred.meta.Config import Resource

# Mock out of scope modules before importing DataExportService
with mock.patch.dict("sys.modules", {"mantid.api": mock.Mock()}):
    from snapred.backend.dao.calibration.CalibrationIndexEntry import CalibrationIndexEntry  # noqa: E402
    from snapred.backend.dao.calibration.CalibrationRecord import CalibrationRecord  # noqa: E402
    from snapred.backend.dao.ReductionIngredients import ReductionIngredients  # noqa: E402
    from snapred.backend.data.LocalDataService import LocalDataService  # noqa: E402

    @mock.patch.object(LocalDataService, "__init__", lambda x: None)  # noqa: PT008, ARG005
    def test_readCalibrationIndexMissing():
        localDataService = LocalDataService()
        localDataService.instrumentConfig = mock.Mock()
        localDataService.stateId = mock.Mock()
        localDataService._readReductionParameters = mock.Mock()
        localDataService._constructCalibrationPath = mock.Mock()
        localDataService._constructCalibrationPath.return_value = Resource.getPath("outputs")
        assert len(localDataService.readCalibrationIndex("123")) == 0

    @mock.patch.object(LocalDataService, "__init__", lambda x: None)  # noqa: PT008, ARG005
    def test_writeCalibrationIndexEntry():
        localDataService = LocalDataService()
        localDataService.instrumentConfig = mock.Mock()
        localDataService.stateId = mock.Mock()
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

    @mock.patch.object(LocalDataService, "__init__", lambda x: None)  # noqa: PT008, ARG005
    def test_readCalibrationIndexExisting():
        localDataService = LocalDataService()
        localDataService.instrumentConfig = mock.Mock()
        localDataService.stateId = mock.Mock()
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

    @mock.patch.object(LocalDataService, "__init__", lambda x: None)  # noqa: PT008, ARG005
    def test_readWriteCalibrationRecord():
        localDataService = LocalDataService()
        localDataService.instrumentConfig = mock.Mock()
        localDataService.stateId = mock.Mock()
        localDataService._readReductionParameters = mock.Mock()
        localDataService._constructCalibrationPath = mock.Mock()
        localDataService._constructCalibrationPath.return_value = Resource.getPath("outputs/")
        localDataService.writeCalibrationRecord(CalibrationRecord(parameters=readReductionIngredientsFromFile()))
        actualRecord = localDataService.readCalibrationRecord("57514")
        # remove directory
        shutil.rmtree(Resource.getPath("outputs/57514"))

        assert actualRecord.parameters.runConfig.runNumber == "57514"

    @mock.patch.object(LocalDataService, "__init__", lambda x: None)  # noqa: PT008, ARG005
    def test_readWriteCalibrationRecordV2():
        localDataService = LocalDataService()
        localDataService.instrumentConfig = mock.Mock()
        localDataService.stateId = mock.Mock()
        localDataService._readReductionParameters = mock.Mock()
        localDataService._constructCalibrationPath = mock.Mock()
        localDataService._constructCalibrationPath.return_value = Resource.getPath("outputs/")
        localDataService.writeCalibrationRecord(CalibrationRecord(parameters=readReductionIngredientsFromFile()))
        localDataService.writeCalibrationRecord(CalibrationRecord(parameters=readReductionIngredientsFromFile()))
        actualRecord = localDataService.readCalibrationRecord("57514")
        shutil.rmtree(Resource.getPath("outputs/57514"))

        assert actualRecord.parameters.runConfig.runNumber == "57514"
