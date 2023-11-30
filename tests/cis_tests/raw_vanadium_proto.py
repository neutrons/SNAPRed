# this is a test of the raw vanadium correction algorithm
# this in a very lazy test, which copy/pastes over the unit test then runs it

from mantid.simpleapi import *

import json

from snapred.backend.dao.ingredients import ReductionIngredients as Ingredients
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
Resource._resourcesPath = os.path.expanduser("~/SNAPRed/tests/resources/")

# Inputs
VRun = 57473
VBRun = 57472
liteMode=True
# File defining reduction parameters for this instrument state (Manually spec'ed for now)
sPrmFile = '/SNS/SNAP/shared/Calibration/Powder/04bd2c53f6bf6754/057514/SNAPcalibLog57514.lite.json' 
#iPrmFile = '/SNS/SNAP/shared/Calibration/SNAPInstPrm.json'
idf = '/SNS/SNAP/shared/Calibration_old/SNAPLite.xml'
#############################Don't edit below here#########################


#state parameters
with open(sPrmFile, "r") as json_file:
  sPrm = json.load(json_file) 

xmin = sPrm['tofMin']
xmax = sPrm['tofMax']
xbin = (xmax-xmin)/xmin/1000
TOFBinParams = (xmin, xbin, xmax)
print(TOFBinParams)
#print(geomCalibFile)
print('/SNS/SNAP/shared/Calibration/Powder/04bd2c53f6bf6754/057514/SNAP057514_calib_geom_20230324.lite.h5')

# LOAD ALL NEEDED DATA  ########################################################

# Getting reduction ingredients for VRun through DataFactoryService doesn't work.
# As the service tries to locate various files, at one point it needs Config "instrument.home" path to be "/SNS/SNAP/", 
# but at another point it needs it to be "~/SNAPRed/tests/resources/". 
ingredients = Ingredients.parse_raw(Resource.read("/inputs/reduction/input_ingredients.json"))

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
algo.setProperty("InputWorkspace", groceries[0])
algo.setProperty("BackgroundWorkspace", groceries[1])
algo.setProperty("CalibrationWorkspace", difcWS)
algo.setProperty("Ingredients", ingredients.json())
algo.setProperty("CalibrantSample", calibrantSample.json())
algo.setProperty("OutputWorkspace", outputWS)
assert algo.execute()

ConvertUnits(
   InputWorkspace = outputWS,
   OutputWorkspace = outputWS,
   Target = 'dSpacing')  

DiffractionFocussing(
    InputWorkspace = outputWS,
    OutputWorkspace = '_test_focussed',
    GroupingWorkspace = groceries[2],
)

