import unittest
from unittest import mock

import pytest
from mantid.simpleapi import DeleteWorkspace, mtd
from snapred.backend.dao.calibration.Calibration import Calibration
from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.service.SousChef import SousChef


class TestSousChef(unittest.TestCase):
    def setUp(self):
        self.instance = SousChef()

    def tearDown(self):
        del self.instance

    @classmethod
    def tearDownClass(cls):
        for ws in mtd.getObjectNames():
            DeleteWorkspace(ws)

    def test_getCalibration_nocache(self):
        runNumber = "123"
        assert self.instance._calibrationCache == {}

        mockCalibration = mock.Mock()
        self.instance.dataFactoryService.getCalibrationState = mock.Mock(return_value=mockCalibration)

        res = self.instance.prepCalibration(runNumber)

        assert self.instance.dataFactoryService.getCalibrationState.called_once_with(runNumber)
        assert res == self.instance.dataFactoryService.getCalibrationState.return_value

    def test_getCalibration_cached(self):
        runNumber = "123"
        self.instance._calibrationCache = {runNumber: mock.Mock()}
        self.instance.dataFactoryService.getCalibrationState = mock.Mock()

        res = self.instance.prepCalibration(runNumber)
        assert not self.instance.dataFactoryService.getCalibrationState.called
        assert res == self.instance._calibrationCache[runNumber]

    def test_getInstrumentState(self):
        runNumber = "123"
        mockCalibration = mock.Mock(instrumentState=mock.Mock())
        self.instance.prepCalibration = mock.Mock(return_value=mockCalibration)
        res = self.instance.prepInstrumentState(runNumber)
        assert self.instance.prepCalibration.called_once_with(runNumber)
        assert res == self.instance.prepCalibration.return_value.instrumentState
