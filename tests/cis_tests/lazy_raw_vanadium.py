# this is a test of the raw vanadium correction algorithm
# this in a very lazy test, which copy/pastes over the unit test then runs it

from mantid.simpleapi import *
import matplotlib.pyplot as plt
import numpy as np

import json
import random
import unittest
import unittest.mock as mock
from typing import Dict, List

import os
os.environ["env"] = "test"

import pytest
from mantid.api import PythonAlgorithm
from mantid.kernel import Direction

from snapred.backend.dao.DetectorPeak import DetectorPeak
from snapred.backend.dao.GroupPeakList import GroupPeakList
from snapred.backend.dao.ingredients import ReductionIngredients as Ingredients
from snapred.backend.recipe.algorithm.WashDishes import WashDishes

# needed to make mocked ingredients
from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.dao.state.CalibrantSample.Atom import Atom
from snapred.backend.dao.state.CalibrantSample.CalibrantSamples import CalibrantSamples
from snapred.backend.dao.state.CalibrantSample.Crystallography import Crystallography
from snapred.backend.dao.state.CalibrantSample.Geometry import Geometry
from snapred.backend.dao.state.CalibrantSample.Material import Material

# the algorithm to test
from snapred.backend.recipe.algorithm.RawVanadiumCorrectionAlgorithm import RawVanadiumCorrectionAlgorithm as Algo  # noqa: E402
from snapred.meta.Config import Config, Resource
Config._config['cis_mode'] = True
Resource._resourcesPath = "/SNS/users/4rx/SNAPRed/tests/resources/"

TheAlgorithmManager: str = "snapred.backend.recipe.algorithm.MantidSnapper.AlgorithmManager"


class TestRawVanadiumCorrection(unittest.TestCase):
    def setUp(self):
        """Create a set of mocked ingredients for calculating DIFC corrected by offsets"""
        self.fakeRunNumber = "555"
        fakeRunConfig = RunConfig(runNumber=str(self.fakeRunNumber))

        print("/home/4rx/SNAPRed/tests/resources/inputs/reduction")
        print(Resource.getPath("inputs/reduction/fake_file.json"))
        print(Resource.exists("inputs/reduction/fake_file.json"))

        self.fakeIngredients = Ingredients.parse_raw(Resource.read("inputs/reduction/fake_file.json"))
        self.fakeIngredients.runConfig = fakeRunConfig
        TOFBinParams = (1, 0.01, 100)
        self.fakeIngredients.reductionState.stateConfig.tofMin = TOFBinParams[0]
        self.fakeIngredients.reductionState.stateConfig.tofBin = TOFBinParams[1]
        self.fakeIngredients.reductionState.stateConfig.tofMax = TOFBinParams[2]

        # create some nonsense material and crystallography
        fakeMaterial = Material(
            packingFraction=0.3,
            massDensity=1.0,
            chemicalFormula="V-B",
        )
        vanadiumAtom = Atom(
            symbol="V",
            coordinates=[0, 0, 0],
            siteOccupationFactor=0.5,
        )
        boronAtom = Atom(
            symbol="B",
            coordinates=[0, 1, 0],
            siteOccupationFactor=1.0,
        )
        fakeXtal = Crystallography(
            cifFile=Resource.getPath("inputs/crystalInfo/example.cif"),
            spaceGroup="I m -3 m",
            latticeParameters=[1, 2, 3, 4, 5, 6],
            atoms=[vanadiumAtom, boronAtom],
        )
        cylinder = Geometry(
            shape="Cylinder",
            radius=1.5,
            height=5.0,
        )
        self.calibrantSample = CalibrantSamples(
            name="fake cylinder sample",
            unique_id="435elmst",
            geometry=cylinder,
            material=fakeMaterial,
            crystallography=fakeXtal,
        )

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
            Xmin=TOFBinParams[0],
            Xmax=TOFBinParams[2],
            BinWidth=TOFBinParams[1],
            XUnit="TOF",
            NumBanks=4,  # must produce same number of pixels as fake instrument
            BankPixelWidth=2,  # each bank has 4 pixels, 4 banks, 16 total
            Random=True,
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
            Xmin=TOFBinParams[0],
            Xmax=TOFBinParams[2],
            BinWidth=TOFBinParams[1],
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
        # WashDishes(Workspace="_tmp_raw_vanadium")

        self.difcWS = "_difc_table_raw_vanadium"
        ws = CalculateDIFC(
            InputWorkspace=self.sampleWS,
            OutputWorkspace=self.difcWS,
            OffsetMode="Signed",
            BinWidth=TOFBinParams[1],
        )
        difc = CreateEmptyTableWorkspace()
        difc.addColumn("int", "detid")
        difc.addColumn("double", "difc")
        difc.addColumn("double", "tzero")
        difc.addColumn("double", "difa")
        for i in range(ws.getNumberHistograms()):
            difc.addRow([i + 1, ws.readY(i)[0], 0.0, 0.0])
        # WashDishes(Workspace=self.difcWS)
        self.difcWS = difc.name()

        Rebin(
            InputWorkspace=self.sampleWS,
            Params=TOFBinParams,
            PreserveEvents=False,
            OutputWorkspace=self.sampleWS,
            BinningMode="Logarithmic",
        )
        Rebin(
            InputWorkspace=self.backgroundWS,
            Params=TOFBinParams,
            PreserveEvents=False,
            OutputWorkspace=self.backgroundWS,
            BinningMode="Logarithmic",
        )

    def tearDown(self) -> None:
        WashDishes(self.sampleWS)
        return super().tearDown()

    def test_execute(self):
        """Test that the algorithm executes"""
        algo = Algo()
        algo.initialize()
        algo.setProperty("InputWorkspace", self.sampleWS)
        algo.setProperty("BackgroundWorkspace", self.backgroundWS)
        algo.setProperty("CalibrationWorkspace", self.difcWS)
        algo.setProperty("Ingredients", self.fakeIngredients.json())
        algo.setProperty("CalibrantSample", self.calibrantSample.json())
        algo.setProperty("OutputWorkspace", self.outputWS)
        assert algo.execute()


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
