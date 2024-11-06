import datetime

from mantid.simpleapi import (
    CreateSampleWorkspace,
    DeleteWorkspace,
    LoadInstrument,
    mtd,
)

from snapred.backend.dao.state import DetectorState
from snapred.backend.data.util.mapping_util import *
from snapred.meta.Config import Resource

import unittest
from unittest import mock
import pytest

class TestMappingFromRun(unittest.TestCase):

    def setUp(self):
        self.wsName = mtd.unique_hidden_name()
        CreateSampleWorkspace(
            OutputWorkspace=self.wsName,
            # WorkspaceType="Histogram",
            Function="User Defined",
            UserDefinedFunction="name=Gaussian,Height=10,PeakCentre=1.2,Sigma=0.2",
            Xmin=0,
            Xmax=5,
            BinWidth=0.001,
            XUnit="dSpacing",
            NumBanks=4,  # must produce same number of pixels as fake instrument
            BankPixelWidth=2,  # each bank has 4 pixels, 4 banks, 16 total
        )
        LoadInstrument(
            Workspace=self.wsName,
            Filename=Resource.getPath("inputs/testInstrument/fakeSNAP_Definition.xml"),
            RewriteSpectraMap=True,
        )
        self.ws = mtd[self.wsName]

    def tearDown(self):
        DeleteWorkspace(self.wsName)
        
    def test_init(self):
        map = mappingFromRun(self.ws.getRun())

    def test_get_item(self):
        map = mappingFromRun(self.ws.getRun())
        
        assert map["run_title"] == "Test Workspace"
        assert map["start_time"] == np.datetime64("2010-01-01T00:00:00", "ns")
        assert map["end_time"] == np.datetime64("2010-01-01T01:00:00", "ns")
        
    def test_iter(self):
        map = mappingFromRun(self.ws.getRun())
        assert [k for k in map] == ['run_title', 'start_time', 'end_time', 'run_start', 'run_end']
    
    def test_len(self):
        map = mappingFromRun(self.ws.getRun())
        assert len(map) == 5
        
    @mock.patch("mantid.api.Run.hasProperty")
    def test_contains(self, mockHasProperty):
        mockHasProperty.return_value = True
        map = mappingFromRun(self.ws.getRun())
        assert "anything" in map
        mockHasProperty.assert_called_once_with("anything")
        
    @mock.patch("mantid.api.Run.keys")
    def test_keys(self, mockKeys):
        mockKeys.return_value = ["we", "are", "the", "keys"]
        map = mappingFromRun(self.ws.getRun())
        assert map.keys() == mockKeys.return_value
        mockKeys.assert_called_once()


class TestMappingFromNeXusLogs(unittest.TestCase):
        
    def _mockPVFile(self, detectorState: DetectorState) -> mock.Mock:
        # Note: `mapping_util.mappingFromNeXusLogs` will open the 'entry/DASlogs' group,
        #   so this `dict` mocks the HDF5 group, not the PV-file itself.
        
        dict_ = {
            "BL3:Chop:Skf1:WavelengthUserReq/value": [detectorState.wav],
            "det_arc1/value": [detectorState.arc[0]],
            "det_arc2/value": [detectorState.arc[1]],
            "BL3:Det:TH:BL:Frequency/value": [detectorState.freq],
            "BL3:Mot:OpticsPos:Pos/value": [detectorState.guideStat],
            "det_lin1/value": [detectorState.lin[0]],
            "det_lin2/value": [detectorState.lin[1]],
        }

        def del_item(key: str):
            # bypass <class>.__delitem__
            del dict_[key]

        mock_ = mock.MagicMock(spec=h5py.Group)
                        
        mock_.get = lambda key, default=None: dict_.get(key, default)
        mock_.del_item = del_item
    
        # Use of the h5py.File starts with access to the "entry/DASlogs" group:
        mock_.__getitem__.side_effect =\
            lambda key: mock_ if key == "entry/DASlogs" else dict_[key]
        
        mock_.__setitem__.side_effect = dict_.__setitem__
        mock_.__contains__.side_effect = dict_.__contains__
        mock_.keys.side_effect = dict_.keys
        return mock_

    def setUp(self):
        self.detectorState = DetectorState(arc=(1.0, 2.0), wav=1.1, freq=1.2, guideStat=1, lin=(1.0, 2.0))
        self.mockPVFile = self._mockPVFile(self.detectorState)

    def tearDown(self):
        pass
        
    def test_init(self):
        map = mappingFromNeXusLogs(self.mockPVFile)

    def test_get_item(self):
        map = mappingFromNeXusLogs(self.mockPVFile)
        assert map["BL3:Chop:Skf1:WavelengthUserReq"][0] == 1.1

    def test_get_item_key_error(self):
        map = mappingFromNeXusLogs(self.mockPVFile)
        with pytest.raises(KeyError, match="something else"):
            map["something else"]
        
    def test_iter(self):
        map = mappingFromNeXusLogs(self.mockPVFile)
        
        # For each key, the ending "/value" should have been removed
        assert [k for k in map] == [
             'BL3:Chop:Skf1:WavelengthUserReq', 
             'det_arc1',
             'det_arc2',
             'BL3:Det:TH:BL:Frequency',
             'BL3:Mot:OpticsPos:Pos',
             'det_lin1',
             'det_lin2'
        ]
    
    def test_len(self):
        map = mappingFromNeXusLogs(self.mockPVFile)
        assert len(map) == 7
        
    def test_contains(self):
        map = mappingFromNeXusLogs(self.mockPVFile)
        assert "BL3:Chop:Skf1:WavelengthUserReq" in map
        self.mockPVFile.__contains__.assert_called_once_with("BL3:Chop:Skf1:WavelengthUserReq/value")
        
    def test_contains_False(self):
        map = mappingFromNeXusLogs(self.mockPVFile)
        assert "anything" not in map
        self.mockPVFile.__contains__.assert_called_once_with("anything/value")
        
    def test_keys(self):
        map = mappingFromNeXusLogs(self.mockPVFile)
        
        # For each key, the ending "/value" should have been removed
        assert map.keys() == [
             'BL3:Chop:Skf1:WavelengthUserReq', 
             'det_arc1',
             'det_arc2',
             'BL3:Det:TH:BL:Frequency',
             'BL3:Mot:OpticsPos:Pos',
             'det_lin1',
             'det_lin2'
        ]        
        self.mockPVFile.keys.assert_called_once()
