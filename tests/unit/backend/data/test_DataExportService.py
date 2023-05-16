import unittest.mock as mock

import pytest

# Mock out of scope modules before importing DataExportService
# mock.patch("snapred.backend.data"] = mock.Mock()
mock.patch("snapred.backend.data.LocalDataService")
mock.patch("snapred.backend.log")
mock.patch("snapred.backend.log.logger")

from snapred.backend.dao.calibration.CalibrationIndexEntry import CalibrationIndexEntry  # noqa: E402
from snapred.backend.data.DataExportService import DataExportService  # noqa: E402


def teardown():
    """Teardown after all tests"""
    mock.patch.stopall()


@pytest.fixture(autouse=True)
def setup_teardown():  # noqa: PT004
    """Setup before each test, teardown after each test"""
    yield
    teardown()


# test export calibration
def test_exportCalibration():
    dataExportService = DataExportService()
    dataExportService.dataService.writeCalibrationIndexEntry = mock.Mock()
    dataExportService.dataService.writeCalibrationIndexEntry.return_value = "expected"
    dataExportService.exportCalibrationIndexEntry(CalibrationIndexEntry(runNumber="1", comments="", author=""))
    assert dataExportService.dataService.writeCalibrationIndexEntry.called
