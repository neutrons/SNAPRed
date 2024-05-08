"""
this is a test of the raw vanadium correction algorithm
this in a very lazy test, which copy/pastes over the unit test then runs it
the purpose is to manually inspect the output workspaces and ensure the operation performed makes sense
"""

# the algorithm to test
import snapred.backend.recipe.algorithm.RawVanadiumCorrectionAlgorithm
from mantid.simpleapi import *
import matplotlib.pyplot as plt
import numpy as np

import json
import random
import unittest
import unittest.mock as mock
from typing import Dict, List

import pytest
from mantid.api import PythonAlgorithm
from mantid.kernel import Direction

from snapred.backend.dao.DetectorPeak import DetectorPeak
from snapred.backend.dao.GroupPeakList import GroupPeakList
from snapred.backend.dao.ingredients import ReductionIngredients as Ingredients
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper

# needed to make mocked ingredients
import sys
from pathlib import Path
import snapred
SNAPRed_module_root = Path(snapred.__file__).parent.parent
sys.path.insert(0, str(Path(SNAPRed_module_root).parent / 'tests'))
from util.SculleryBoy import SculleryBoy

from snapred.meta.Config import Config, Resource
Config._config['cis_mode'] = True
Resource._resourcesPath = os.path.expanduser("~/SNAPRed/tests/resources/")

class TestRawVanadiumCorrection(unittest.TestCase):
    def setUp(self):
        """Create a set of mocked ingredients for calculating DIFC corrected by offsets"""
        self.ingredients = SculleryBoy().prepNormalizationIngredients({})
        TOFBinParams = (1, 0.01, 100)
        self.ingredients.pixelGroup.timeOfFlight.minimum = TOFBinParams[0]
        self.ingredients.pixelGroup.timeOfFlight.binWidth = TOFBinParams[1]
        self.ingredients.pixelGroup.timeOfFlight.maximum = TOFBinParams[2]
        tof = self.ingredients.pixelGroup.timeOfFlight

        self.sample_proton_charge = 10.0

        # create some sample data
        self.backgroundWS = "_lazy_data_raw_vanadium_background"
        self.sampleWS = "_lazy_data_raw_vanadium"
        self.outputWS = "_lazy_raw_vanadium_final_output"
        self.signalWS = "_lazy_raw_vanadium_signal"
        CreateSampleWorkspace(
            OutputWorkspace=self.backgroundWS,
            # WorkspaceType="Histogram",
            Function="User Defined",
            UserDefinedFunction="name=Gaussian,Height=10,PeakCentre=30,Sigma=1",
            Xmin=tof.minimum,
            Xmax=tof.maximum,
            BinWidth=1,
            XUnit="TOF",
            NumBanks=4,  # must produce same number of pixels as fake instrument
            BankPixelWidth=2,  # each bank has 4 pixels, 4 banks, 16 total
            Random=False,
        )
        # add proton charge for current normalization
        AddSampleLog(
            Workspace=self.backgroundWS,
            LogName="gd_prtn_chrg",
            LogText=f"{self.sample_proton_charge}",
            LogType="Number",
        )
        # load an instrument into sample data
        LoadInstrument(
            Workspace=self.backgroundWS,
            Filename=Resource.getPath("inputs/testInstrument/fakeSNAP.xml"),
            InstrumentName="fakeSNAP",
            RewriteSpectraMap=False,
        )

        CloneWorkspace(
            InputWorkspace=self.backgroundWS,
            OutputWorkspace=self.sampleWS,
        )
        CreateSampleWorkspace(
            OutputWorkspace=self.signalWS,
            # WorkspaceType="Histogram",
            Function="User Defined",
            UserDefinedFunction="name=Gaussian,Height=10,PeakCentre=70,Sigma=1",
            Xmin=tof.minimum,
            Xmax=tof.maximum,
            BinWidth=1,
            XUnit="TOF",
            NumBanks=4,  # must produce same number of pixels as fake instrument
            BankPixelWidth=2,  # each bank has 4 pixels, 4 banks, 16 total
            Random=False,
        )
        Plus(
            LHSWorkspace=self.signalWS, 
            RHSWorkspace=self.sampleWS, 
            OutputWorkspace=self.sampleWS,
        )

        Rebin(
            InputWorkspace=self.sampleWS,
            Params=tof.params,
            PreserveEvents=False,
            OutputWorkspace=self.sampleWS,
            BinningMode="Logarithmic",
        )
        Rebin(
            InputWorkspace=self.backgroundWS,
            Params=tof.params,
            PreserveEvents=False,
            OutputWorkspace=self.backgroundWS,
            BinningMode="Logarithmic",
        )

    def test_execute(self):
        """Test that the algorithm executes"""
        assert RawVanadiumCorrectionAlgorithm(
            InputWorkspace = self.sampleWS,
            BackgroundWorkspace = self.backgroundWS,
            Ingredients = self.ingredients.json(),
            OutputWorkspace = self.outputWS,
        )

test = TestRawVanadiumCorrection()
test.setUp()
test.test_execute()

fig, ax = plt.subplots(subplot_kw={'projection':'mantid'})
ax.plot(mtd[test.backgroundWS], wkspIndex=2, label="background")
ax.plot(mtd[test.signalWS], wkspIndex=2, label="signal")
ax.plot(mtd[test.sampleWS], wkspIndex=2, label="raw data")
ax.plot(mtd[test.outputWS], wkspIndex=2, label="corrected data")
ax.legend() # show the legend
fig.show()
