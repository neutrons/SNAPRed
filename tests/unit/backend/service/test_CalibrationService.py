import unittest.mock as mock

import pytest

# Mock out of scope modules before importing DataExportService
mock.patch("snapred.backend.data.DataExportService")
mock.patch("snapred.backend.data.DataFactoryService")
mock.patch("snapred.backend.recipe.CalibrationReductionRecipe")
mock.patch("snapred.backend.log")
mock.patch("snapred.backend.log.logger")

from snapred.backend.dao.calibration.CalibrationIndexEntry import CalibrationIndexEntry  # noqa: E402
from snapred.backend.service.CalibrationService import CalibrationService  # noqa: E402


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
    dataExportService = CalibrationService()
    dataExportService.dataExportService.exportCalibrationIndexEntry = mock.Mock()
    dataExportService.dataExportService.exportCalibrationIndexEntry.return_value = "expected"
    dataExportService.saveCalibrationToIndex(CalibrationIndexEntry(runNumber="1", comments="", author=""))
    assert dataExportService.dataExportService.exportCalibrationIndexEntry.called
    savedEntry = dataExportService.dataExportService.exportCalibrationIndexEntry.call_args.args[0]
    assert savedEntry.appliesTo == ">1"
    assert savedEntry.timestamp is not None
