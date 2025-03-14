from collections.abc import Iterable
import numpy as np
from typing import Any, Dict, List

from mantid.kernel import DateAndTime
from mantid.simpleapi import (
    CreateSampleWorkspace,
    DeleteWorkspace,
    DeleteWorkspaces,
    LoadInstrument,
    mtd,
)

from snapred.backend.dao.state import DetectorState
from snapred.backend.data.util.PV_logs_util import *
from snapred.meta.Config import Config, Resource

##
## In order to keep the rest of the import sequence unmodified: any test-related imports are added at the end.
##
import unittest
import pytest
from unittest import mock
from util.dao import DAOFactory
from util.instrument_helpers import addInstrumentLogs, getInstrumentLogDescriptors

class TestTransferInstrumentPVLogs(unittest.TestCase):
    @classmethod
    def createSampleWorkspace(cls):
        wsName = mtd.unique_hidden_name()
        CreateSampleWorkspace(
            OutputWorkspace=wsName,
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
            Workspace=wsName,
            Filename=Resource.getPath("inputs/testInstrument/fakeSNAP_Definition.xml"),
            RewriteSpectraMap=True,
        )
        return wsName

    def setUp(self):
        self.wsWithStandardLogs = self.createSampleWorkspace()

        # Add the standard instrument PV-logs to the workspace's `Run` attribute.
        self.detectorState = DAOFactory.real_detector_state
        self.instrumentKeys = [
            k if not isinstance(k, List) else k[0] for k in Config["instrument.PVLogs.instrumentKeys"]
        ]
        logsDescriptors = getInstrumentLogDescriptors(self.detectorState)
        addInstrumentLogs(self.wsWithStandardLogs, **logsDescriptors)
        self.standardLogs = dict(zip(logsDescriptors["logNames"], logsDescriptors["logValues"]))

        # Add the alterate instrument PV-logs.
        self.wsWithAlternateLogs = self.createSampleWorkspace()
        self.alternateInstrumentKeys = [
            k if k != "BL3:Chop:Skf1:WavelengthUserReq" else "BL3:Chop:Gbl:WavelengthReq" for k in self.instrumentKeys
        ]
        logsDescriptors["logNames"] = [
            k if k != "BL3:Chop:Skf1:WavelengthUserReq" else "BL3:Chop:Gbl:WavelengthReq"
            for k in logsDescriptors["logNames"]
        ]
        addInstrumentLogs(self.wsWithAlternateLogs, **logsDescriptors)
        self.alternateLogs = dict(zip(logsDescriptors["logNames"], logsDescriptors["logValues"]))

    def tearDown(self):
        DeleteWorkspaces(WorkspaceList=[self.wsWithStandardLogs, self.wsWithAlternateLogs])

    def test_Config_keys(self):
        # Verify that the standard instrument PV-logs have been attached to the test workspace.
        # (This test additionally verifies that the `addInstrumentLogs` interface is using the keys from `Config`.)
        run = mtd[self.wsWithStandardLogs].run()
        for key in self.instrumentKeys:
            assert run.hasProperty(key)
            assert f"{run.getProperty(key).value[0]:.16f}" == self.standardLogs[key]

        # Verify the test workspace with the alternate instrument PV-logs.
        run = mtd[self.wsWithAlternateLogs].run()
        for key in self.alternateInstrumentKeys:
            assert run.hasProperty(key)
            assert f"{run.getProperty(key).value[0]:.16f}" == self.alternateLogs[key]

    def verify_transfer(self, srcWs: str, keys: Iterable, alternateKeys: Iterable):
        testWs = self.createSampleWorkspace()
        ws = mtd[testWs]
        for key in keys:
            assert not ws.run().hasProperty(key)

        transferInstrumentPVLogs(mtd[testWs].mutableRun(), mtd[srcWs].run(), keys)
        populateInstrumentParameters(testWs)

        run = mtd[testWs].run()
        srcRun = mtd[srcWs].run()

        # Verify the log transfer.
        for key in keys:
            assert run.hasProperty(key)
            assert run.getProperty(key).value == srcRun.getProperty(key).value

        # Verify that there are no extra "alternate" entries.
        for key in alternateKeys:
            if key not in keys:
                assert not run.hasProperty(key)

        DeleteWorkspace(testWs)

    def test_transfer(self):
        self.verify_transfer(self.wsWithStandardLogs, self.instrumentKeys, self.alternateInstrumentKeys)

    def test_alternate_transfer(self):
        self.verify_transfer(self.wsWithAlternateLogs, self.alternateInstrumentKeys, self.instrumentKeys)

    def test_instrument_update(self):
        # The PV-logs are transferred between the workspace's `Run` attributes.
        # Any change in an instrument-related PV-log must also be _applied_ as a transformation
        # to the workspace's parameterized instrument.
        # This test verifies that, following a logs transfer, such an update works correctly.

        testWs = self.createSampleWorkspace()

        # Verify that [some of the] detector pixels of the standard source workspace have been moved
        #   from their original locations. Here, we don't care about the specifics of the transformation.
        originalPixels = mtd[testWs].detectorInfo()
        sourcePixels = mtd[self.wsWithStandardLogs].detectorInfo()
        instrumentUpdateApplied = False
        for n in range(sourcePixels.size()):
            if sourcePixels.position(n) != originalPixels.position(n) or sourcePixels.rotation(
                n
            ) != originalPixels.rotation(n):
                instrumentUpdateApplied = True
                break
        assert instrumentUpdateApplied

        transferInstrumentPVLogs(mtd[testWs].mutableRun(), mtd[self.wsWithStandardLogs].run(), self.instrumentKeys)
        populateInstrumentParameters(testWs)

        # Verify that the same instrument transformation
        #   has been applied to the source and to the destination workspace.
        newPixels = mtd[testWs].detectorInfo()
        sourcePixels = mtd[self.wsWithStandardLogs].detectorInfo()
        instrumentUpdateApplied = True
        for n in range(sourcePixels.size()):
            # If these don't match _exactly_, then the values have not been transferred at full precision.
            if newPixels.position(n) != sourcePixels.position(n) or newPixels.rotation(n) != sourcePixels.rotation(n):
                instrumentUpdateApplied = False
                break
        assert instrumentUpdateApplied


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

    class _mockProperty():

        def __init__(self, value):
            self.value = value

    class _mockRun():

        def __init__(
            self,
            dict_: Dict[str, Any],
            end_time: DateAndTime,
            start_time: DateAndTime,
            proton_charge: float
        ):
            self._dict = dict_
            self._end_time = end_time
            self._start_time = start_time
            self._proton_charge = proton_charge

        def endTime(self) -> DateAndTime:
            return self._end_time

        def startTime(self) -> DateAndTime:
            return self._start_time

        def protonCharge(self) -> float:
            return self._proton_charge

        def hasProperty(self, key: str) -> bool:
            return key in self._dict

        def getProperty(self, key: str) -> "TestMappingFromRun._mockProperty":
            if key not in self._dict:
                raise RuntimeError(f"Unknown property search object {key}")
            return TestMappingFromRun._mockProperty(self._dict[key])

        def keys(self):
            return self._dict.keys()
            
    def test_init(self):
        map_ = mappingFromRun(self.ws.getRun())  # noqa: F841

    def test_get_item(self):
        map_ = mappingFromRun(self.ws.getRun())

        assert map_["run_title"] == "Test Workspace"
        assert map_["start_time"] == np.datetime64("2010-01-01T00:00:00", "ns")
        assert map_["end_time"] == np.datetime64("2010-01-01T01:00:00", "ns")

    def test_iter(self):
        map_ = mappingFromRun(self.ws.getRun())
        expectedKeys = ["run_title", "start_time", "end_time", "proton_charge", "run_number", "run_start", "run_end"]
        for k in map_:
            assert k in expectedKeys

    def test_len(self):
        map_ = mappingFromRun(self.ws.getRun())
        print([k for k in self.ws.getRun().keys()])
        print(map_.keys())
        assert len(map_) == 7

    @mock.patch("mantid.api.Run.hasProperty")
    def test_contains_direct(self, mockHasProperty):
        mockHasProperty.return_value = True
        map_ = mappingFromRun(self.ws.getRun())
        
        # NOTE: `<run>.hasProperty()` is also called during `mappingFromRun.__init__()`.
        mockHasProperty.reset_mock()
        assert "anything" in map_
        mockHasProperty.assert_called_once_with("anything")

    def test_contains_indirect_special_cases(self):
        map_ = mappingFromRun(self.ws.getRun())
        assert "start_time" in map_
        assert "end_time" in map_
        assert "proton_charge" in map_
        assert "run_number" in map_

    def test_contains_primary(self):
        # Test that any '__contains__' with a primary key references the primary-key only.
        """
        EXAMPLE: 'instrument.PVLogs.instrumentKeys' section from 'application.yml':
         -
           - "BL3:Chop:Skf1:WavelengthUserReq"
           - "BL3:Chop:Gbl:WavelengthReq"
           - "BL3:Det:TH:BL:Lambda"
         - "det_arc1"
         - "det_arc2"
         - "BL3:Det:TH:BL:Frequency"
         - 
           - "BL3:Mot:OpticsPos:Pos"
           - "optics"
         - "det_lin1"
         - "det_lin2"       
        """
        _mockRun = TestMappingFromRun._mockRun
        _run = _mockRun(
            {
              "BL3:Chop:Skf1:WavelengthUserReq": [24.0],
              "det_arc1": [1.25],
              "det_arc2": [2.26],
              "BL3:Det:TH:BL:Frequency": [35.0],
              "BL3:Mot:OpticsPos:Pos": [10.0],
              "det_lin1": [1.0],
              "det_lin2": [2.0]
            },
            start_time="2010-01-01T00:00:00",
            end_time="2010-01-01T01:00:00",
            proton_charge=1000.0
        )
        mockRun = mock.Mock(
            wraps=_mockRun,
            return_value=_run
        )
        map_ = mappingFromRun(mockRun())
        for ks in Config["instrument.PVLogs.instrumentKeys"]:
            if isinstance(ks, str):
                # make sure keys have been initialized for this test
                assert ks in _run._dict
                # make sure all keys are in the mapping
                assert ks in map_
            else:
                # make sure that only the primary key is referencable
                assert ks[0] in _run._dict
                assert ks[0] in map_
                for k in ks[1:]:
                    assert not k in map_

    def test_contains_alternatives(self):
        # Test that any '__contains__' with a primary key references the alternative-key.
        """
        EXAMPLE: 'instrument.PVLogs.instrumentKeys' section from 'application.yml':
         -
           - "BL3:Chop:Skf1:WavelengthUserReq"
           - "BL3:Chop:Gbl:WavelengthReq"
           - "BL3:Det:TH:BL:Lambda"
         - "det_arc1"
         - "det_arc2"
         - "BL3:Det:TH:BL:Frequency"
         - 
           - "BL3:Mot:OpticsPos:Pos"
           - "optics"
         - "det_lin1"
         - "det_lin2"       
        """
        _mockRun = TestMappingFromRun._mockRun
        _run = _mockRun(
            {
              "BL3:Chop:Gbl:WavelengthReq": [24.0],
              "det_arc1": [1.25],
              "det_arc2": [2.26],
              "BL3:Det:TH:BL:Frequency": [35.0],
              "optics": [10.0],
              "det_lin1": [1.0],
              "det_lin2": [2.0]
            },
            start_time="2010-01-01T00:00:00",
            end_time="2010-01-01T01:00:00",
            proton_charge=1000.0
        )
        mockRun = mock.Mock(
            wraps=_mockRun,
            return_value=_run
        )
        map_ = mappingFromRun(mockRun())
        for ks in Config["instrument.PVLogs.instrumentKeys"]:
            if isinstance(ks, str):
                # make sure keys have been initialized for this test
                assert ks in _run._dict
                # make sure all keys are in the mapping
                assert ks in map_
            else:
                # primary key references alternative key
                assert not ks[0] in _run._dict
                assert ks[0] in map_
                # alternative key continues to reference itself
                assert ks[1] in map_
                    
    def test_keys_special_cases(self):
        map_ = mappingFromRun(self.ws.getRun())
        keys_ = map_.keys()
        assert "start_time" in keys_
        assert "end_time" in keys_
        assert "proton_charge" in keys_
        assert "run_number" in keys_    
    
    def test_keys_alternatives(self):
        # Test that both primary keys and any existing altarnative-keys show up in the 'keys()' return value.
        """
        EXAMPLE: 'instrument.PVLogs.instrumentKeys' section from 'application.yml':
         -
           - "BL3:Chop:Skf1:WavelengthUserReq"
           - "BL3:Chop:Gbl:WavelengthReq"
           - "BL3:Det:TH:BL:Lambda"
         - "det_arc1"
         - "det_arc2"
         - "BL3:Det:TH:BL:Frequency"
         - 
           - "BL3:Mot:OpticsPos:Pos"
           - "optics"
         - "det_lin1"
         - "det_lin2"       
        """
        _mockRun = TestMappingFromRun._mockRun
        _run = _mockRun(
            {
              "BL3:Chop:Gbl:WavelengthReq": [24.0],
              "det_arc1": [1.25],
              "det_arc2": [2.26],
              "BL3:Det:TH:BL:Frequency": [35.0],
              "optics": [10.0],
              "det_lin1": [1.0],
              "det_lin2": [2.0]
            },
            start_time="2010-01-01T00:00:00",
            end_time="2010-01-01T01:00:00",
            proton_charge=1000.0
        )
        mockRun = mock.Mock(
            wraps=_mockRun,
            return_value=_run
        )
        map_ = mappingFromRun(mockRun())
        keys_ = map_.keys()
        for ks in Config["instrument.PVLogs.instrumentKeys"]:
            if isinstance(ks, str):
                # make sure keys have been initialized for this test
                assert ks in _run._dict
                # make sure all keys are in the mapping
                assert ks in keys_
            else:
                # primary key references alternative key
                assert not ks[0] in _run._dict
                assert ks[0] in keys_
                # alternative key continues to reference itself
                assert ks[1] in keys_

    def test_get_alternatives(self):
        # Test that any 'get' with a primary key references the altarnative-key value.
        """
        EXAMPLE: 'instrument.PVLogs.instrumentKeys' section from 'application.yml':
         -
           - "BL3:Chop:Skf1:WavelengthUserReq"
           - "BL3:Chop:Gbl:WavelengthReq"
           - "BL3:Det:TH:BL:Lambda"
         - "det_arc1"
         - "det_arc2"
         - "BL3:Det:TH:BL:Frequency"
         - 
           - "BL3:Mot:OpticsPos:Pos"
           - "optics"
         - "det_lin1"
         - "det_lin2"       
        """
        _mockRun = TestMappingFromRun._mockRun
        _run = _mockRun(
            {
              "BL3:Chop:Gbl:WavelengthReq": [24.0],
              "det_arc1": [1.25],
              "det_arc2": [2.26],
              "BL3:Det:TH:BL:Frequency": [35.0],
              "optics": [10.0],
              "det_lin1": [1.0],
              "det_lin2": [2.0]
            },
            start_time="2010-01-01T00:00:00",
            end_time="2010-01-01T01:00:00",
            proton_charge=1000.0
        )
        mockRun = mock.Mock(
            wraps=_mockRun,
            return_value=_run
        )
        map_ = mappingFromRun(mockRun())
        for ks in Config["instrument.PVLogs.instrumentKeys"]:
            if isinstance(ks, str):
                assert map_[ks] == _run._dict[ks]
            else:
                # primary key references alternative key
                assert map_[ks[0]] == _run._dict[ks[1]]
                # alternative key continues to reference itself
                assert map_[ks[1]] == _run._dict[ks[1]]
        
    def test_default_run_number(self):
        # test that default 'run_number' is int(0)
        _mockRun = TestMappingFromRun._mockRun
        _run = _mockRun(
            {
              "BL3:Chop:Skf1:WavelengthUserReq": [24.0],
              "det_arc1": [1.25],
              "det_arc2": [2.26],
              "BL3:Det:TH:BL:Frequency": [35.0],
              "BL3:Mot:OpticsPos:Pos": [10.0],
              "det_lin1": [1.0],
              "det_lin2": [2.0]
            },
            start_time="2010-01-01T00:00:00",
            end_time="2010-01-01T01:00:00",
            proton_charge=1000.0
        )
        mockRun = mock.Mock(
            wraps=_mockRun,
            return_value=_run
        )
        map_ = mappingFromRun(mockRun())
        assert "run_number" in map_
        assert map_["run_number"] == 0
        
    def test_run_number(self):
        # test that 'run_number' is recognized correctly
        runNumber = "12345"
        _mockRun = TestMappingFromRun._mockRun
        _run = _mockRun(
            {
              "BL3:Chop:Skf1:WavelengthUserReq": [24.0],
              "det_arc1": [1.25],
              "det_arc2": [2.26],
              "BL3:Det:TH:BL:Frequency": [35.0],
              "BL3:Mot:OpticsPos:Pos": [10.0],
              "det_lin1": [1.0],
              "det_lin2": [2.0],
              "run_number": runNumber
            },
            start_time="2010-01-01T00:00:00",
            end_time="2010-01-01T01:00:00",
            proton_charge=1000.0
        )
        mockRun = mock.Mock(
            wraps=_mockRun,
            return_value=_run
        )
        map_ = mappingFromRun(mockRun())
        assert "run_number" in map_
        assert map_["run_number"] == runNumber
        
    @mock.patch("mantid.api.Run.keys")
    def test_keys(self, mockKeys):
        mockKeys.return_value = ["we", "are", "the", "keys"]
        map_ = mappingFromRun(self.ws.getRun())
        expectedKeys = set(mockKeys.return_value)
        expectedKeys.update(["start_time", "end_time", "proton_charge", "run_number"])
        assert set(map_.keys()) == expectedKeys
        mockKeys.assert_called_once()


class TestMappingFromNeXusLogs(unittest.TestCase):
    def _mockPVFile(self, detectorState: DetectorState) -> mock.Mock:
        # Note: `PV_logs_util.mappingFromNeXusLogs` will open the 'entry/DASlogs' group,
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
        mock_.__getitem__.side_effect = lambda key: mock_ if key == "entry/DASlogs" else dict_[key]

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
        map_ = mappingFromNeXusLogs(self.mockPVFile)  # noqa: F841

    def test_get_item(self):
        map_ = mappingFromNeXusLogs(self.mockPVFile)
        assert map_["BL3:Chop:Skf1:WavelengthUserReq"][0] == 1.1

    def test_get_item_key_error(self):
        map_ = mappingFromNeXusLogs(self.mockPVFile)
        with pytest.raises(KeyError, match="something else"):
            map_["something else"]

    def test_iter(self):
        map_ = mappingFromNeXusLogs(self.mockPVFile)

        # For each key, the ending "/value" should have been removed
        assert [k for k in map_] == [
            "BL3:Chop:Skf1:WavelengthUserReq",
            "det_arc1",
            "det_arc2",
            "BL3:Det:TH:BL:Frequency",
            "BL3:Mot:OpticsPos:Pos",
            "det_lin1",
            "det_lin2",
        ]

    def test_len(self):
        map_ = mappingFromNeXusLogs(self.mockPVFile)
        assert len(map_) == 7

    def test_contains(self):
        map_ = mappingFromNeXusLogs(self.mockPVFile)
        assert "BL3:Chop:Skf1:WavelengthUserReq" in map_
        self.mockPVFile.__contains__.assert_called_once_with("BL3:Chop:Skf1:WavelengthUserReq/value")

    def test_contains_False(self):
        map_ = mappingFromNeXusLogs(self.mockPVFile)
        assert "anything" not in map_
        self.mockPVFile.__contains__.assert_called_once_with("anything/value")

    def test_keys(self):
        map_ = mappingFromNeXusLogs(self.mockPVFile)

        # For each key, the ending "/value" should have been removed
        assert map_.keys() == [
            "BL3:Chop:Skf1:WavelengthUserReq",
            "det_arc1",
            "det_arc2",
            "BL3:Det:TH:BL:Frequency",
            "BL3:Mot:OpticsPos:Pos",
            "det_lin1",
            "det_lin2",
        ]
        self.mockPVFile.keys.assert_called_once()
