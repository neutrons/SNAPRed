import logging

##
## In order to keep the rest of the import sequence unmodified: any test-related imports are added at the end.
##
import unittest
from collections.abc import Iterable
from typing import Any, Dict, List
from unittest import mock

import numpy as np
import pytest
from mantid.api import MatrixWorkspace, WorkspaceGroup
from mantid.dataobjects import TableWorkspace
from mantid.kernel import DateAndTime
from mantid.simpleapi import (
    CreateSampleWorkspace,
    DeleteWorkspace,
    DeleteWorkspaces,
    LoadInstrument,
    mtd,
)
from util.dao import DAOFactory
from util.instrument_helpers import addInstrumentLogs, getInstrumentLogDescriptors

from snapred.backend.dao.state import DetectorState
from snapred.backend.data.util.PV_logs_util import *
from snapred.meta.Config import Config, Resource


def test_allInstrumentPVLogKeys():
    keysWithAlternates = ["one", ["two", "three", "four"], "five", ["six", "seven"], "eight"]
    expected = []
    for ks in keysWithAlternates:
        if isinstance(ks, str):
            expected.append(ks)
        else:
            expected.extend(ks)
    # We do not care about the ordering, just that all keys are present.
    assert set(allInstrumentPVLogKeys(keysWithAlternates)) == set(expected)


def test_allInstrumentPVLogKeys_format_error():
    keysWithAlternates = ["one", [["two", "three", "four"], "one"], "five", ["six", "seven"], "eight"]
    with pytest.raises(RuntimeError, match="unexpected format: list of keys with alternates"):
        keys = allInstrumentPVLogKeys(keysWithAlternates)  # noqa: F841


class TestPopulateInstrumentParameters(unittest.TestCase):
    class _MockMtd:
        # In order to test flow-of-control, the only thing that matters is
        #   the _type_ of the workspace.

        @classmethod
        def _mockWorkspace(cls, wsClass, wsName, wsNames):
            # return a mock where `isinstance(mock, wsClass) == True`
            #   and <mock>.getTitle() == wsName
            _mock = mock.Mock(spec=wsClass, getTitle=mock.Mock(return_value=wsName), run=mock.Mock())
            if wsClass is WorkspaceGroup:
                assert wsNames is not None
                _mock.getNames = mock.Mock(return_value=wsNames)
            assert isinstance(_mock, wsClass)
            return _mock

        def __init__(self, wsNames=None):
            self._wsNames = wsNames

        def __getitem__(self, name):
            ws = None
            if "group" in name:
                ws = self._mockWorkspace(WorkspaceGroup, name, self._wsNames)
            elif "table" in name:
                ws = self._mockWorkspace(TableWorkspace, name, self._wsNames)
            else:
                ws = self._mockWorkspace(MatrixWorkspace, name, self._wsNames)
            return ws

    def test_single_workspace(self):
        # test `populateInstrumentParameters` with a single `MatrixWorkspace`.
        mockSnapper_ = mock.Mock()
        mockSnapper_.mtd = TestPopulateInstrumentParameters._MockMtd()
        assert isinstance(mockSnapper_.mtd["test_workspace"], MatrixWorkspace)

        with (
            mock.patch("snapred.backend.data.util.PV_logs_util._snapper", mockSnapper_) as mockSnapper,
        ):
            populateInstrumentParameters("test_workspace")
            mockSnapper.AddSampleLog.assert_called_once()
            mockSnapper.executeQueue.assert_called_once()
            # Verify that `ExperimentInfo::populateInstrumentParameters` was triggered.
            assert mockSnapper.AddSampleLog.mock_calls[0].kwargs["UpdateInstrumentParameters"]

    @pytest.mark.skip(reason="TODO: Breaks 'TestTransferInstrumentPVLogs.test_instrument_update'?!")
    def test_snapper_import(self):
        # test `populateInstrumentParameters` import of `MantidSnapper`.
        mockSnapper_ = mock.Mock()
        mockSnapper_.mtd = TestPopulateInstrumentParameters._MockMtd()
        assert isinstance(mockSnapper_.mtd["test_workspace"], MatrixWorkspace)

        with mock.patch.dict(
            "snapred.backend.recipe.algorithm.MantidSnapper.MantidSnapper", mock.Mock(return_value=mockSnapper_)
        ) as mockSnapperClass:
            populateInstrumentParameters("test_workspace")
            mockSnapperClass.assert_called_once()

    def test_single_table_workspace(self):
        # test `AddSampleLog` is not called on a single `TableWorkspace`.
        mockSnapper_ = mock.Mock()
        mockSnapper_.mtd = TestPopulateInstrumentParameters._MockMtd()
        assert isinstance(mockSnapper_.mtd["test_table"], TableWorkspace)

        with (
            mock.patch("snapred.backend.data.util.PV_logs_util._snapper", mockSnapper_) as mockSnapper,
        ):
            populateInstrumentParameters("test_table")
            mockSnapper.AddSampleLog.assert_not_called()

    def test_group_workspace(self):
        # test `populateInstrumentParameters` is called on each workspace in a `WorkspaceGroup`.
        mockSnapper_ = mock.Mock()
        mockSnapper_.mtd = TestPopulateInstrumentParameters._MockMtd(wsNames=["ws1", "ws2", "ws3", "ws4", "ws5"])
        assert isinstance(mockSnapper_.mtd["test_group"], WorkspaceGroup)
        N_wss = len(mockSnapper_.mtd["test_group"].getNames())

        with (
            mock.patch("snapred.backend.data.util.PV_logs_util._snapper", mockSnapper_) as mockSnapper,
        ):
            populateInstrumentParameters("test_group")
            # Verify that `AddSampleLog` is called for every workspace in the group.
            mockSnapper.AddSampleLog.call_count == N_wss
            for n in range(len(mockSnapper.AddSampleLog.mock_calls)):
                # Verify that `populateInstrumentParameters` is triggered for each call.
                assert mockSnapper.AddSampleLog.mock_calls[n].kwargs["UpdateInstrumentParameters"]

    def test_group_workspace_with_table(self):
        # test `AddSampleLog` is not called for any `TableWorkspace` in a `WorkspaceGroup`.
        mockSnapper_ = mock.Mock()
        mockSnapper_.mtd = TestPopulateInstrumentParameters._MockMtd(wsNames=["ws1", "ws2", "table_ws3", "ws4", "ws5"])
        assert isinstance(mockSnapper_.mtd["test_group"], WorkspaceGroup)

        with (
            mock.patch("snapred.backend.data.util.PV_logs_util._snapper", mockSnapper_) as mockSnapper,
        ):
            populateInstrumentParameters("test_group")
            mockSnapper.AddSampleLog.call_count == len(
                [ws for ws in mockSnapper_.mtd["test_group"].getNames() if "table" not in ws]
            )

    def test_group_workspace_all_tables(self):
        # test `AddSampleLog` is not called at all for a group consisting of all `TableWorkspace`.
        mockSnapper_ = mock.Mock()
        mockSnapper_.mtd = TestPopulateInstrumentParameters._MockMtd(
            wsNames=["table_ws1", "table_ws2", "table_ws3", "table_ws4", "table_ws5"]
        )
        assert isinstance(mockSnapper_.mtd["test_group"], WorkspaceGroup)

        with (
            mock.patch("snapred.backend.data.util.PV_logs_util._snapper", mockSnapper_) as mockSnapper,
        ):
            populateInstrumentParameters("test_group")
            mockSnapper.AddSampleLog.assert_not_called()


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

    class _mockProperty:
        def __init__(self, value):
            self.value = value

    class _MockRun:
        def __init__(
            self,
            dict_: Dict[str, Any],
            end_time: DateAndTime = None,
            start_time: DateAndTime = None,
            proton_charge: float = None,
        ):
            self._dict = dict_
            self._end_time = end_time
            self._start_time = start_time
            self._proton_charge = proton_charge

        def endTime(self) -> DateAndTime:
            if self._end_time is None:
                raise RuntimeError("no end time has been set for this run")
            return self._end_time

        def startTime(self) -> DateAndTime:
            if self._start_time is None:
                raise RuntimeError("no start time has been set for this run")
            return self._start_time

        def getProtonCharge(self) -> float:
            if self._proton_charge is None:
                raise RuntimeError("proton charge log is empty")
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
        _MockRun = TestMappingFromRun._MockRun
        _run = _MockRun(
            {
                "BL3:Chop:Skf1:WavelengthUserReq": [24.0],
                "det_arc1": [1.25],
                "det_arc2": [2.26],
                "BL3:Det:TH:BL:Frequency": [35.0],
                "BL3:Mot:OpticsPos:Pos": [10.0],
                "det_lin1": [1.0],
                "det_lin2": [2.0],
            },
            start_time="2010-01-01T00:00:00",
            end_time="2010-01-01T01:00:00",
            proton_charge=1000.0,
        )
        mockRun = mock.Mock(wraps=_MockRun, return_value=_run)
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
                    assert k not in map_

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
        _MockRun = TestMappingFromRun._MockRun
        _run = _MockRun(
            {
                "BL3:Chop:Gbl:WavelengthReq": [24.0],
                "det_arc1": [1.25],
                "det_arc2": [2.26],
                "BL3:Det:TH:BL:Frequency": [35.0],
                "optics": [10.0],
                "det_lin1": [1.0],
                "det_lin2": [2.0],
            },
            start_time="2010-01-01T00:00:00",
            end_time="2010-01-01T01:00:00",
            proton_charge=1000.0,
        )
        mockRun = mock.Mock(wraps=_MockRun, return_value=_run)
        map_ = mappingFromRun(mockRun())
        for ks in Config["instrument.PVLogs.instrumentKeys"]:
            if isinstance(ks, str):
                # make sure keys have been initialized for this test
                assert ks in _run._dict
                # make sure all keys are in the mapping
                assert ks in map_
            else:
                # primary key references alternative key
                assert ks[0] not in _run._dict
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
        _MockRun = TestMappingFromRun._MockRun
        _run = _MockRun(
            {
                "BL3:Chop:Gbl:WavelengthReq": [24.0],
                "det_arc1": [1.25],
                "det_arc2": [2.26],
                "BL3:Det:TH:BL:Frequency": [35.0],
                "optics": [10.0],
                "det_lin1": [1.0],
                "det_lin2": [2.0],
            },
            start_time="2010-01-01T00:00:00",
            end_time="2010-01-01T01:00:00",
            proton_charge=1000.0,
        )
        mockRun = mock.Mock(wraps=_MockRun, return_value=_run)
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
                assert ks[0] not in _run._dict
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
        _MockRun = TestMappingFromRun._MockRun
        _run = _MockRun(
            {
                "BL3:Chop:Gbl:WavelengthReq": [24.0],
                "det_arc1": [1.25],
                "det_arc2": [2.26],
                "BL3:Det:TH:BL:Frequency": [35.0],
                "optics": [10.0],
                "det_lin1": [1.0],
                "det_lin2": [2.0],
            },
            start_time="2010-01-01T00:00:00",
            end_time="2010-01-01T01:00:00",
            proton_charge=1000.0,
        )
        mockRun = mock.Mock(wraps=_MockRun, return_value=_run)
        map_ = mappingFromRun(mockRun())
        for ks in Config["instrument.PVLogs.instrumentKeys"]:
            if isinstance(ks, str):
                assert map_[ks] == _run._dict[ks]
            else:
                # primary key references alternative key
                assert map_[ks[0]] == _run._dict[ks[1]]
                # alternative key continues to reference itself
                assert map_[ks[1]] == _run._dict[ks[1]]

    def test_get_key_error(self):
        # Test that "Unknown property search object" is converted to `KeyError`.
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
        _MockRun = TestMappingFromRun._MockRun
        _run = _MockRun(
            {
                "BL3:Chop:Gbl:WavelengthReq": [24.0],
                "det_arc1": [1.25],
                "det_arc2": [2.26],
                "BL3:Det:TH:BL:Frequency": [35.0],
                "optics": [10.0],
                "det_lin1": [1.0],
                "det_lin2": [2.0],
            },
            start_time="2010-01-01T00:00:00",
            end_time="2010-01-01T01:00:00",
            proton_charge=1000.0,
        )
        mockRun = mock.Mock(wraps=_MockRun, return_value=_run)
        mockRun.return_value.getProperty = mock.Mock(side_effect=RuntimeError("Unknown property search object"))
        map_ = mappingFromRun(mockRun())
        with pytest.raises(KeyError, match="lah_dee_dah"):
            value = map_["lah_dee_dah"]  # noqa: F841

    def test_get_not_key_error(self):
        # Test that only "property not found" is converted to `KeyError`.
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
        _MockRun = TestMappingFromRun._MockRun
        _run = _MockRun(
            {
                "BL3:Chop:Gbl:WavelengthReq": [24.0],
                "det_arc1": [1.25],
                "det_arc2": [2.26],
                "BL3:Det:TH:BL:Frequency": [35.0],
                "optics": [10.0],
                "det_lin1": [1.0],
                "det_lin2": [2.0],
            },
            start_time="2010-01-01T00:00:00",
            end_time="2010-01-01T01:00:00",
            proton_charge=1000.0,
        )
        mockRun = mock.Mock(wraps=_MockRun, return_value=_run)
        mockRun.return_value.getProperty = mock.Mock(side_effect=RuntimeError("something bad happened"))
        map_ = mappingFromRun(mockRun())
        with pytest.raises(RuntimeError, match="something bad happened"):
            value = map_["lah_dee_dah"]  # noqa: F841

    def test_default_start_and_end_times(self):
        # test that default start and end times are "now"
        _MockRun = TestMappingFromRun._MockRun
        _run = _MockRun(
            {
                "BL3:Chop:Skf1:WavelengthUserReq": [24.0],
                "det_arc1": [1.25],
                "det_arc2": [2.26],
                "BL3:Det:TH:BL:Frequency": [35.0],
                "BL3:Mot:OpticsPos:Pos": [10.0],
                "det_lin1": [1.0],
                "det_lin2": [2.0],
                "run_number": "12345",
            },
            start_time=None,
            end_time=None,
            proton_charge=1000.0,
        )
        mockRun = mock.Mock(wraps=_MockRun, return_value=_run)
        map_ = mappingFromRun(mockRun())
        assert "start_time" in map_
        assert map_["start_time"] == map_._now
        assert "end_time" in map_
        assert map_["end_time"] == map_._now

    def test_default_proton_charge(self):
        # test that proton charge is zero
        _MockRun = TestMappingFromRun._MockRun
        _run = _MockRun(
            {
                "BL3:Chop:Skf1:WavelengthUserReq": [24.0],
                "det_arc1": [1.25],
                "det_arc2": [2.26],
                "BL3:Det:TH:BL:Frequency": [35.0],
                "BL3:Mot:OpticsPos:Pos": [10.0],
                "det_lin1": [1.0],
                "det_lin2": [2.0],
                "run_number": "12345",
            },
            start_time="2010-01-01T00:00:00",
            end_time="2010-01-01T01:00:00",
            proton_charge=None,
        )
        mockRun = mock.Mock(wraps=_MockRun, return_value=_run)
        map_ = mappingFromRun(mockRun())
        assert "proton_charge" in map_
        assert map_["proton_charge"] == 0.0

    def test_default_run_number(self):
        # test that default 'run_number' is int(0)
        _MockRun = TestMappingFromRun._MockRun
        _run = _MockRun(
            {
                "BL3:Chop:Skf1:WavelengthUserReq": [24.0],
                "det_arc1": [1.25],
                "det_arc2": [2.26],
                "BL3:Det:TH:BL:Frequency": [35.0],
                "BL3:Mot:OpticsPos:Pos": [10.0],
                "det_lin1": [1.0],
                "det_lin2": [2.0],
            },
            start_time="2010-01-01T00:00:00",
            end_time="2010-01-01T01:00:00",
            proton_charge=1000.0,
        )
        mockRun = mock.Mock(wraps=_MockRun, return_value=_run)
        map_ = mappingFromRun(mockRun())
        assert "run_number" in map_
        assert map_["run_number"] == 0

    def test_defaults_warning(self):
        # test that default 'run_number' is int(0)
        _MockRun = TestMappingFromRun._MockRun
        _run = _MockRun(
            {
                "BL3:Chop:Skf1:WavelengthUserReq": [24.0],
                "det_arc1": [1.25],
                "det_arc2": [2.26],
                "BL3:Det:TH:BL:Frequency": [35.0],
                "BL3:Mot:OpticsPos:Pos": [10.0],
                "det_lin1": [1.0],
                "det_lin2": [2.0],
            },
            start_time=None,
            end_time=None,
            proton_charge=None,
        )
        mockRun = mock.Mock(wraps=_MockRun, return_value=_run)
        mockLogger_ = mock.Mock(isEnabledFor=mock.Mock(return_value=True))
        with mock.patch("snapred.backend.data.util.PV_logs_util.logger", mockLogger_) as mockLogger:
            map_ = mappingFromRun(mockRun())
            startTime = map_["start_time"]  # noqa: F841
            endTime = map_["end_time"]  # noqa: F841
            protonCharge = map_["proton_charge"]  # noqa: F841
            assert mockLogger.isEnabledFor.call_count == 3
            mockLogger.isEnabledFor.assert_any_call(logging.DEBUG)
            assert mockLogger.warning.call_count == 3
            assert "start time" in mockLogger.warning.mock_calls[0].args[0]
            assert "end time" in mockLogger.warning.mock_calls[1].args[0]
            assert "proton charge" in mockLogger.warning.mock_calls[2].args[0]

    def test_run_number(self):
        # test that 'run_number' is recognized correctly
        runNumber = "12345"
        _MockRun = TestMappingFromRun._MockRun
        _run = _MockRun(
            {
                "BL3:Chop:Skf1:WavelengthUserReq": [24.0],
                "det_arc1": [1.25],
                "det_arc2": [2.26],
                "BL3:Det:TH:BL:Frequency": [35.0],
                "BL3:Mot:OpticsPos:Pos": [10.0],
                "det_lin1": [1.0],
                "det_lin2": [2.0],
                "run_number": runNumber,
            },
            start_time="2010-01-01T00:00:00",
            end_time="2010-01-01T01:00:00",
            proton_charge=1000.0,
        )
        mockRun = mock.Mock(wraps=_MockRun, return_value=_run)
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
    def _mockPVFile(self, detectorState: DetectorState, hasTitle: bool) -> mock.Mock:
        def _mockH5Dataset(array_value):
            ds = mock.MagicMock(spec=h5py.Dataset)
            ds.__getitem__.side_effect = lambda x: array_value  # noqa: ARG005
            ds.shape = array_value.shape
            ds.dtype = array_value.dtype
            return ds

        dict_ = {
            "BL3:Chop:Skf1:WavelengthUserReq/value": _mockH5Dataset(np.array([detectorState.wav])),
            "det_arc1/value": np.array([detectorState.arc[0]]),
            "det_arc2/value": np.array([detectorState.arc[1]]),
            "BL3:Det:TH:BL:Frequency/value": np.array([detectorState.freq]),
            "BL3:Mot:OpticsPos:Pos/value": np.array([detectorState.guideStat]),
            "det_lin1/value": np.array([detectorState.lin[0]]),
            "det_lin2/value": np.array([detectorState.lin[1]]),
        }
        if hasTitle:
            dict_["/entry/title"] = _mockH5Dataset(np.array([b"MyTestTitle"]))
        mock_ = mock.MagicMock(spec=h5py.Group)

        def side_effect_getitem(key: str):
            if key == "entry/DASlogs":
                return mock_
            else:
                return dict_[key]

        mock_.__getitem__.side_effect = side_effect_getitem
        mock_.__setitem__.side_effect = dict_.__setitem__
        mock_.__contains__.side_effect = dict_.__contains__
        mock_.keys.side_effect = dict_.keys
        return mock_

    def setUp(self):
        self.detectorState = DetectorState(arc=(1.0, 2.0), wav=1.1, freq=1.2, guideStat=1, lin=(1.0, 2.0))
        self.mockPVFile = self._mockPVFile(self.detectorState, hasTitle=False)

    def tearDown(self):
        pass

    def test_init(self):
        mockFile = self._mockPVFile(self.detectorState, hasTitle=False)
        map_ = mappingFromNeXusLogs(mockFile)  # noqa: F841

    def test_get_item(self):
        mockFile = self._mockPVFile(self.detectorState, hasTitle=False)
        map_ = mappingFromNeXusLogs(mockFile)
        assert map_["BL3:Chop:Skf1:WavelengthUserReq"][0] == 1.1

    def test_get_item_title(self):
        mockFile = self._mockPVFile(self.detectorState, hasTitle=True)
        map_ = mappingFromNeXusLogs(mockFile)
        assert "title" in map_
        titleValue = map_["title"]
        assert titleValue[0] == b"MyTestTitle"

    def test_keys_includes_title(self):
        mockFile = self._mockPVFile(self.detectorState, hasTitle=True)
        map_ = mappingFromNeXusLogs(mockFile)
        keys_ = map_.keys()
        assert "title" in keys_
        for expected in [
            "BL3:Chop:Skf1:WavelengthUserReq",
            "det_arc1",
            "det_arc2",
            # etc
        ]:
            assert expected in keys_

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
