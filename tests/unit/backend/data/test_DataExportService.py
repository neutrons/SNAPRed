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
    def test_exportCalibrationIndexEntry():
        dataExportService = DataExportService()
        dataExportService.dataService.writeCalibrationIndexEntry = mock.Mock()
        dataExportService.dataService.writeCalibrationIndexEntry.return_value = "expected"
        dataExportService.exportCalibrationIndexEntry(CalibrationIndexEntry(runNumber="1", comments="", author=""))
        assert dataExportService.dataService.writeCalibrationIndexEntry.called

    def test_exportCalibrationRecord():
        dataExportService = DataExportService()
        dataExportService.dataService.writeCalibrationRecord = mock.Mock()
        dataExportService.dataService.writeCalibrationRecord.return_value = "expected"
        dataExportService.exportCalibrationRecord(mock.Mock())
        assert dataExportService.dataService.writeCalibrationRecord.called

    def test_exportCalibrationReductionResult():
        dataExportService = DataExportService()
        dataExportService.dataService.writeCalibrationReductionResult = mock.Mock()
        dataExportService.dataService.writeCalibrationReductionResult.return_value = "expected"
        dataExportService.exportCalibrationReductionResult(mock.Mock(), mock.Mock())
        assert dataExportService.dataService.writeCalibrationReductionResult.called

    def test_exportCalibrationState():
        dataExportService = DataExportService()
        dataExportService.dataService.writeCalibrationState = mock.Mock()
        dataExportService.dataService.writeCalibrationState.return_value = "expected"
        dataExportService.exportCalibrationState(mock.Mock(), mock.Mock())
        assert dataExportService.dataService.writeCalibrationState.called

    def test_initializeState():
        dataExportService = DataExportService()
        dataExportService.dataService.initializeState = mock.Mock()
        dataExportService.dataService.initializeState.return_value = "expected"
        dataExportService.initializeState(mock.Mock(), mock.Mock())
        assert dataExportService.dataService.initializeState.called
