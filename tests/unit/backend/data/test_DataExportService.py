
import pytest
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
    from snapred.backend.dao.normalization.NormalizationIndexEntry import NormalizationIndexEntry  # noqa: E402
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

    def test_exportCalibrationWorkspaces():
        dataExportService = DataExportService()
        dataExportService.dataService.writeCalibrationWorkspaces = mock.Mock()
        dataExportService.dataService.writeCalibrationWorkspaces.return_value = "expected"
        dataExportService.exportCalibrationWorkspaces(mock.Mock())
        assert dataExportService.dataService.writeCalibrationWorkspaces.called

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

    def test_exportNormalizationIndexEntry():
        dataExportService = DataExportService()
        dataExportService.dataService.writeNormalizationIndexEntry = mock.Mock()
        dataExportService.dataService.writeNormalizationIndexEntry.return_value = "expected"
        dataExportService.exportNormalizationIndexEntry(
            NormalizationIndexEntry(runNumber="1", backgroundRunNumber="2", comments="", author="")
        )
        assert dataExportService.dataService.writeNormalizationIndexEntry.called

    def test_exportNormalizationRecord():
        dataExportService = DataExportService()
        dataExportService.dataService.writeNormalizationRecord = mock.Mock()
        dataExportService.dataService.writeNormalizationRecord.return_value = "expected"
        dataExportService.exportNormalizationRecord(mock.Mock())
        assert dataExportService.dataService.writeNormalizationRecord.called

    def test_exportNormalizationWorkspaces():
        dataExportService = DataExportService()
        dataExportService.dataService.writeNormalizationWorkspaces = mock.Mock()
        dataExportService.dataService.writeNormalizationWorkspaces.return_value = "expected"
        dataExportService.exportNormalizationWorkspaces(mock.Mock())
        assert dataExportService.dataService.writeNormalizationWorkspaces.called

# this at teardown removes the loggers, eliminating logger error printouts
# see https://github.com/pytest-dev/pytest/issues/5502#issuecomment-647157873
@pytest.fixture(autouse=True)
def clear_loggers():  # noqa: PT004
    """Remove handlers from all loggers"""
    import logging

    yield  # ... teardown follows:
    loggers = [logging.getLogger()] + list(logging.Logger.manager.loggerDict.values())
    for logger in loggers:
        handlers = getattr(logger, "handlers", [])
        for handler in handlers:
            logger.removeHandler(handler)
