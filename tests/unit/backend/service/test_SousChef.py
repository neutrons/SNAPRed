import unittest
from unittest import mock

import pytest
from mantid.simpleapi import DeleteWorkspace, mtd
from snapred.backend.dao.calibration.Calibration import Calibration
from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.service.SousChef import SousChef


class TestSousChef(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.instance = SousChef()

    def setUp(self):
        self.instance.__pixelGroupCache = {}
        self.instance.__calibrationCache = {}
        self.instance.__peaksCache = {}
        self.instance.__xtalCache = {}

    @classmethod
    def tearDownClass(cls):
        for ws in mtd.getObjectNames():
            DeleteWorkspace(ws)

    @mock.patch("snapred.backend.data.DataFactoryService")
    def test_getCalibration_nocache(self, mockDataService):
        runNumber = "123"
        assert self.instance.__calibrationCache == {}
        res = self.instance.getCalibration(runNumber)
        assert mockDataService.called
        assert mockDataService.return_value.getCalibrationState.called_once_with(runNumber)
        assert res == mockDataService.return_value.getCalibrationState.return_value

    def test_getCalibration_cached(self):
        runNumber = "123"
        self.instance.__calibrationCache[runNumber] = mock.Mock(spec_set=Calibration)
        res = self.instance.getCalibration(runNumber)
        assert res == self.instance.__calibrationCache[runNumber]

    def test_getInstrumentState(self):
        runNumber = "123"
        self.instance.getCalibration = mock.Mock(return_value=mock.Mock(spec_set=Calibration))
        res = self.instance.getInstrumentState(runNumber)
        assert self.instance.getCalibration.called_once_with(runNumber)
        assert res == self.instance.getCalibration.return_value.instrumentState

    def test_groupingSchemaFromPath(self):
        apple = "apple"
        path = f"path/to/file_{apple}.biscuit"
        res = self.instance.groupingSchemaFromPath(path)
        assert res == apple

    def test_getFocusGroup(self):
        pass
