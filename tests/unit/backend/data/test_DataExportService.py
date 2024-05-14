import unittest
import unittest.mock as mock
from pathlib import Path

from snapred.backend.dao.calibration.CalibrationIndexEntry import CalibrationIndexEntry  # noqa: E402
from snapred.backend.dao.normalization.NormalizationIndexEntry import NormalizationIndexEntry  # noqa: E402
from snapred.backend.data.DataExportService import DataExportService  # noqa: E402
from snapred.backend.data.LocalDataService import LocalDataService


class TestDataExportService(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """
        Create a data service, which is the underlying service called by the data exporter
        """
        cls.mockLookupService = mock.create_autospec(LocalDataService, spec_set=True, instance=True)

    def setUp(self):
        self.instance = DataExportService(dataService=self.mockLookupService)
        # self.instance.lookupService = self.mockLookupService
        assert isinstance(self.instance, DataExportService)
        return super().setUp()

    def tearDown(self):
        """At the end of each test, clear out the workspaces"""
        del self.instance
        return super().tearDown()

    ##### TEST MISCELLANEOUS METHODS #####

    def test_exportCalibrantSampleFile(self):
        pass

    ##### TEST REDUCTION METHODS #####

    # NOTE will exist in future

    ##### TEST CALIBRATION METHODS #####

    def test_exportCalibrationIndexEntry(self):
        self.instance.exportCalibrationIndexEntry(
            CalibrationIndexEntry(runNumber="1", useLiteMode=True, comments="", author="")
        )
        assert self.instance.dataService.writeCalibrationIndexEntry.called

    def test_exportCalibrationRecord(self):
        self.instance.exportCalibrationRecord(mock.Mock())
        assert self.instance.dataService.writeCalibrationRecord.called

    def test_exportCalibrationWorkspaces(self):
        self.instance.exportCalibrationWorkspaces(mock.Mock())
        assert self.instance.dataService.writeCalibrationWorkspaces.called

    def test_exportCalibrationState(self):
        self.instance.exportCalibrationState(mock.Mock())
        assert self.instance.dataService.writeCalibrationState.called

    def test_initializeState(self):
        self.instance.initializeState("123", False, "nope")
        assert self.instance.dataService.initializeState.called

    ##### TEST NORMALIZATION METHODS #####

    def test_exportNormalizationIndexEntry(self):
        self.instance.exportNormalizationIndexEntry(
            NormalizationIndexEntry(runNumber="1", useLiteMode=True, backgroundRunNumber="2", comments="", author="")
        )
        assert self.instance.dataService.writeNormalizationIndexEntry.called

    def test_exportNormalizationRecord(self):
        self.instance.exportNormalizationRecord(mock.Mock())
        assert self.instance.dataService.writeNormalizationRecord.called

    def test_exportNormalizationState(self):
        self.instance.exportNormalizationState(mock.Mock())
        assert self.instance.dataService.writeNormalizationState.called

    def test_exportNormalizationWorkspaces(self):
        self.instance.exportNormalizationWorkspaces(mock.Mock())
        assert self.instance.dataService.writeNormalizationWorkspaces.called

    ##### TEST WORKSPACE METHODS #####

    def test_exportWorkspace(self):
        self.instance.exportWorkspace(Path(), Path(), "")
        assert self.instance.dataService.writeWorkspace.called

    def test_exportRaggedWorkspace(self):
        self.instance.exportRaggedWorkspace(Path(), Path(), "")
        assert self.instance.dataService.writeRaggedWorkspace.called
