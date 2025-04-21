##
## In order to keep the rest of the import sequence unmodified: any test-related imports are added at the end.
##
import unittest
from collections.abc import Iterable
from typing import List
from unittest import mock

import pytest
from mantid.api import MatrixWorkspace, WorkspaceGroup
from mantid.dataobjects import TableWorkspace
from mantid.simpleapi import (
    CreateSampleWorkspace,
    DeleteWorkspace,
    DeleteWorkspaces,
    LoadInstrument,
    mtd,
)
from util.dao import DAOFactory
from util.instrument_helpers import addInstrumentLogs, getInstrumentLogDescriptors

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
