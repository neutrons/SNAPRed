from collections.abc import Iterable
import datetime

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

# In order to keep the rest of the import sequence unmodified: any test-related imports are added at the end.
import unittest
from unittest import mock
import pytest
from util.dao import DAOFactory
from util.instrument_helpers import (
    getInstrumentLogDescriptors,
    addInstrumentLogs
)

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
        self.standardLogs = dict(zip(logsDescriptors['logNames'], logsDescriptors['logValues']))

        # Add the alterate instrument PV-logs.
        self.wsWithAlternateLogs = self.createSampleWorkspace()
        self.alternateInstrumentKeys = [
            k for k in Config["instrument.PVLogs.instrumentKeys"] if k != "BL3:Chop:Skf1:WavelengthUserReq"
        ]        
        logsDescriptors["logNames"] = [
            k if k != "BL3:Chop:Skf1:WavelengthUserReq" else "BL3:Chop:Gbl:WavelengthReq"\
                for k in logsDescriptors["logNames"]
        ]
        addInstrumentLogs(self.wsWithAlternateLogs, **logsDescriptors)
        self.alternateLogs = dict(zip(logsDescriptors['logNames'], logsDescriptors['logValues']))
        
        
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
        
        transferInstrumentPVLogs(
            mtd[testWs].mutableRun(),
            mtd[srcWs].run(),
            keys
        )
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
            if sourcePixels.position(n) != originalPixels.position(n)\
              or sourcePixels.rotation(n) != originalPixels.rotation(n):
                instrumentUpdateApplied = True
                break
        assert instrumentUpdateApplied
        
        transferInstrumentPVLogs(
            mtd[testWs].mutableRun(),
            mtd[self.wsWithStandardLogs].run(),
            self.instrumentKeys
        )
        populateInstrumentParameters(testWs)
        
        # Verify that the same instrument transformation
        #   has been applied to the source and to the destination workspace.
        newPixels = mtd[testWs].detectorInfo()
        sourcePixels = mtd[self.wsWithStandardLogs].detectorInfo()
        instrumentUpdateApplied = True
        for n in range(sourcePixels.size()):
            # If these don't match _exactly_, then the values have not been transferred at full precision.
            if newPixels.position(n) != sourcePixels.position(n)\
              or newPixels.rotation(n) != sourcePixels.rotation(n):
                instrumentUpdateApplied = False
                break
        assert instrumentUpdateApplied
            
