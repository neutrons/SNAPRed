##
## In order to keep the rest of the import sequence unmodified: any test-related imports are added at the end.
##
import unittest
from collections.abc import Iterable
from unittest import mock

import numpy as np
import pytest
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
            k for k in Config["instrument.PVLogs.instrumentKeys"] if k != "BL3:Chop:Gbl:WavelengthReq"
        ]
        logsDescriptors = getInstrumentLogDescriptors(self.detectorState)
        addInstrumentLogs(self.wsWithStandardLogs, **logsDescriptors)
        self.standardLogs = dict(zip(logsDescriptors["logNames"], logsDescriptors["logValues"]))

        # Add the alterate instrument PV-logs.
        self.wsWithAlternateLogs = self.createSampleWorkspace()
        self.alternateInstrumentKeys = [
            k for k in Config["instrument.PVLogs.instrumentKeys"] if k != "BL3:Chop:Skf1:WavelengthUserReq"
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
        for key in Config["instrument.PVLogs.instrumentKeys"]:
            if key == "BL3:Chop:Gbl:WavelengthReq":
                continue
            assert run.hasProperty(key)
            assert f"{run.getProperty(key).value[0]:.16f}" == self.standardLogs[key]

        # Verify the test workspace with the alternate instrument PV-logs.
        run = mtd[self.wsWithAlternateLogs].run()
        for key in Config["instrument.PVLogs.instrumentKeys"]:
            if key == "BL3:Chop:Skf1:WavelengthUserReq":
                continue
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

    def test_init(self):
        map_ = mappingFromRun(self.ws.getRun())  # noqa: F841

    def test_get_item(self):
        map_ = mappingFromRun(self.ws.getRun())

        assert map_["run_title"] == "Test Workspace"
        assert map_["start_time"] == np.datetime64("2010-01-01T00:00:00", "ns")
        assert map_["end_time"] == np.datetime64("2010-01-01T01:00:00", "ns")

    def test_iter(self):
        map_ = mappingFromRun(self.ws.getRun())
        assert [k for k in map_] == ["run_title", "start_time", "end_time", "run_start", "run_end"]

    def test_len(self):
        map_ = mappingFromRun(self.ws.getRun())
        assert len(map_) == 5

    @mock.patch("mantid.api.Run.hasProperty")
    def test_contains(self, mockHasProperty):
        mockHasProperty.return_value = True
        map_ = mappingFromRun(self.ws.getRun())
        assert "anything" in map_
        mockHasProperty.assert_called_once_with("anything")

    @mock.patch("mantid.api.Run.keys")
    def test_keys(self, mockKeys):
        mockKeys.return_value = ["we", "are", "the", "keys"]
        map_ = mappingFromRun(self.ws.getRun())
        assert map_.keys() == mockKeys.return_value
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
