import sys
import unittest.mock as mock

# Mock out of scope modules before importing DataExportService
sys.modules["snapred.backend.data.DataExportService"] = mock.Mock()
sys.modules["snapred.backend.data.DataFactoryService"] = mock.Mock()
sys.modules["snapred.backend.recipe.CalibrationReductionRecipe"] = mock.Mock()
sys.modules["snapred.backend.log"] = mock.Mock()
sys.modules["snapred.backend.log.logger"] = mock.Mock()

from snapred.backend.dao.calibration.CalibrationIndexEntry import CalibrationIndexEntry  # noqa: E402
from snapred.backend.service.CalibrationService import CalibrationService  # noqa: E402

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
