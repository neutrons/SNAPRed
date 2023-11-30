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


# for loading data
from snapred.backend.dao.ingredients.GroceryListItem import GroceryListItem
from snapred.backend.recipe.FetchGroceriesRecipe import FetchGroceriesRecipe as FetchRx

# the algorithm to test
from snapred.backend.recipe.algorithm.RawVanadiumCorrectionAlgorithm import RawVanadiumCorrectionAlgorithm as Algo  # noqa: E402
from snapred.meta.Config import Config, Resource
Config._config['cis_mode'] = False


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

xmin = sPrm['tofMin']
xmax = sPrm['tofMax']
xbin = (xmax-xmin)/xmin/1000
TOFBinParams = (xmin, xbin, xmax)
print(TOFBinParams)
print(geomCalibFile)
print('/SNS/SNAP/shared/Calibration/Powder/04bd2c53f6bf6754/057514/SNAP057514_calib_geom_20230324.lite.h5')

# LOAD ALL NEEDED DATA  ########################################################
dataFactoryService=DataFactoryService()
ingredients = dataFactoryService.getReductionIngredients(VRun)
ingredients.reductionState.stateConfig.tofMin = TOFBinParams[0]
ingredients.reductionState.stateConfig.tofBin = TOFBinParams[1]
ingredients.reductionState.stateConfig.tofMax = TOFBinParams[2]

difcWS = "_difc"
LoadDiffCal(
    Filename = geomCalibFile,
    MakeCalWorkspace = True,
    WorkspaceName = difcWS,
    InstrumentFilename = idf,
)
difcWS = difcWS + "_cal"

groceryList = [
    GroceryListItem.makeLiteNexusItem(VRun),
    GroceryListItem.makeLiteNexusItem(VBRun),
    GroceryListItem.makeLiteGroupingItemFrom("Column", "prev"),
]
groceries = FetchRx().executeRecipe(groceryList)["workspaces"]

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
outputWS = "_test_raw_vanadium_final_output"
algo = Algo()
algo.initialize()
algo.setProperty("InputWorkspace", groceries[0])
algo.setProperty("BackgroundWorkspace", groceries[1])
algo.setProperty("CalibrationWorkspace", difcWS)
algo.setProperty("Ingredients", ingredients.json())
algo.setProperty("CalibrantSample", calibrantSample.json())
algo.setProperty("OutputWorkspace", outputWS)
assert algo.execute()

DiffractionFocussing(
    InputWorkspace = outputWS,
    OutputWorkspace = '_test_focussed',
    GroupingWorkspace = groceries[2],
)


