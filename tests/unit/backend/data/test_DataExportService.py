import unittest.mock as mock

# Mock out of scope modules before importing DataExportService
# mock.patch("snapred.backend.data"] = mock.Mock()
with mock.patch.dict(
    "sys.modules",
    {
        "snapred.backend.data.LocalDataService": mock.Mock(),
        "snapred.backend.log": mock.Mock(),
        "snapred.backend.log.logger": mock.Mock(),
    },
):
    from snapred.backend.dao.calibration.CalibrationIndexEntry import CalibrationIndexEntry  # noqa: E402
    from snapred.backend.data.DataExportService import DataExportService  # noqa: E402

    # test export calibration
    def test_exportCalibration():
        dataExportService = DataExportService()
        dataExportService.dataService.writeCalibrationIndexEntry = mock.Mock()
        dataExportService.dataService.writeCalibrationIndexEntry.return_value = "expected"
        dataExportService.exportCalibrationIndexEntry(CalibrationIndexEntry(runNumber="1", comments="", author=""))
        assert dataExportService.dataService.writeCalibrationIndexEntry.called
