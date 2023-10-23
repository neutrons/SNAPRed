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

# import os
# os.environ["env"] = "test"

import pytest
from mantid.api import PythonAlgorithm
from mantid.kernel import Direction

from snapred.backend.dao.ingredients import ReductionIngredients as Ingredients
from snapred.backend.data.DataFactoryService import DataFactoryService
from snapred.backend.dao.state.CalibrantSample.CalibrantSamples import CalibrantSamples
from snapred.backend.dao.state.CalibrantSample.Material import Material
from snapred.backend.dao.state.CalibrantSample.Geometry import Geometry

# the algorithm to test
from snapred.backend.recipe.algorithm.RawVanadiumCorrection import RawVanadiumCorrection as Algo  # noqa: E402
from snapred.meta.Config import Config, Resource
Config._config['cis_mode'] = True


# Inputs
VRun = 57473
VBRun = 57472
liteMode=True
# File defining reduction parameters for this instrument state (Manually spec'ed for now)
sPrmFile = '/SNS/SNAP/shared/Calibration/Powder/04bd2c53f6bf6754/057514/SNAPcalibLog57514.lite.json' 
iPrmFile = '/SNS/SNAP/shared/Calibration/SNAPInstPrm.json'
idf = '/SNS/SNAP/shared/Calibration_old/SNAPLite.xml'
#############################Don't edit below here#########################

#instrument parameters
with open(iPrmFile, "r") as json_file:
  iPrm = json.load(json_file) 

#state parameters
with open(sPrmFile, "r") as json_file:
  sPrm = json.load(json_file) 

geomCalibFile= iPrm['calibrationDirectory'] + sPrm['stateID'] +'/057514/'+ sPrm['calFileName']

rawVFile = iPrm['calibrationDirectory'] + sPrm['stateID'] +'/057514/'
if liteMode:
    rawVFile = rawVFile + f'RVMB{VRun}.lite.nxs'
else:
    rawVFile = rawVFile + f'RVMB{VRun}.nxs'

xmin = sPrm['tofMin']
xmax = sPrm['tofMax']
xbin = (xmax-xmin)/xmin/1000
TOFBinParams = (xmin, xbin, xmax)
print(TOFBinParams)


# LOAD ALL NEEDED DATA  ########################################################
dataFactoryService=DataFactoryService()
ingredients = dataFactoryService.getReductionIngredients(VRun)
ingredients.reductionState.stateConfig.tofMin = TOFBinParams[0]
ingredients.reductionState.stateConfig.tofBin = TOFBinParams[1]
ingredients.reductionState.stateConfig.tofMax = TOFBinParams[2]

def loadFromRunNumber(runNo, wsName):
    IPTSLoc = GetIPTS(RunNumber=runNo,Instrument='SNAP')
    if liteMode:
        filename = f'{IPTSLoc}shared/lite/SNAP_{runNo}.lite.nxs.h5'
    else:
        filename = f'{IPTSLoc}nexus/SNAP_{runNo}.nxs.h5'  
    if mtd.doesExist(wsName):
        print("ALREADY EXISTS!")
    else:
        LoadEventNexus(
            Filename=filename, 
            OutputWorkspace=wsName, 
            FilterByTofMin=TOFBinParams[0], 
            FilterByTofMax=TOFBinParams[2], 
            NumberOfBins=1,
        )

rawVanadiumWS = 'z_TOF_V'
rawBackgroundWS = 'z_TOF_VB'
vanadiumWS = '_TOF_V'
backgroundWS = '_TOF_VB'
loadFromRunNumber(VRun, rawVanadiumWS)
loadFromRunNumber(VBRun, rawBackgroundWS)
CloneWorkspace(
    InputWorkspace = rawVanadiumWS,
    OutputWorkspace = vanadiumWS,
)
CloneWorkspace(
    Inputworkspace = rawBackgroundWS,
    OutputWorkspace = backgroundWS,
)

# Resource._resourcesPath = "~/SNAPRed/tests/resources/"
# print(Resource.getPath("inputs/diffcal/SNAPLite_Definition.xml"))
# idf = Resource.getPath("inputs/diffcal/SNAPLite_Definition.xml")

difcWS = "_difc_cal"
LoadDiffCal(
    Filename = geomCalibFile,
    MakeCalWorkspace = True,
    WorkspaceName = "_difc",
    InstrumentFilename = idf,
)


# CREATE MATERIAL ########################################################
material = Material(
    chemicalFormula="V",
)
cylinder = Geometry(
    shape="Cylinder",
    radius=0.15,
    height=0.3,
)
calibrantSample = CalibrantSamples(
    name="vanadium cylinder",
    unique_id="435elmst",
    geometry=cylinder,
    material=material,
)


# RUN ALGO ########################################################
algo = Algo()
algo.initialize()
algo.setProperty("InputWorkspace", vanadiumWS)
algo.setProperty("BackgroundWorkspace", backgroundWS)
algo.setProperty("CalibrationWorkspace", "_difc_cal")
algo.setProperty("Ingredients", ingredients.json())
algo.setProperty("CalibrantSample", calibrantSample.json())
algo.setProperty("OutputWorkspace", "_test_raw_vanadium_final_output")
assert algo.execute()

print(algo.liteMode)

DiffractionFocussing(
    InputWorkspace = "_test_raw_vanadium_final_output",
    OutputWorkspace = '_test_focussed',
    GroupingFilename = '/SNS/SNAP/shared/Calibration_old/PixelGroupingDefinitions/SNAPFocGrp_Column.lite.cal',
)





# SaveNexus(InputWorkspace="_test_raw_vanadium_final_output", Filename=rawVFile)