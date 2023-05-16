import os
import unittest.mock as mock
from typing import List

from pydantic import parse_raw_as

# Mock out of scope modules before importing DataExportService
with mock.patch.dict("sys.modules", {"mantid.api": mock.Mock()}):
    from snapred.backend.dao.calibration.CalibrationIndexEntry import CalibrationIndexEntry  # noqa: E402
    from snapred.backend.data.LocalDataService import LocalDataService  # noqa: E402
    from snapred.meta.Config import Resource  # noqa: E402

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
