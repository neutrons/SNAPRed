# this is a test of the raw vanadium correction algorithm
# this in a very lazy test, which copy/pastes over the unit test then runs it


# the algorithm to test
import snapred.backend.recipe.algorithm.RawVanadiumCorrectionAlgorithm
from mantid.simpleapi import *

import json

# for prepping ingredients
from snapred.backend.dao.ingredients import ReductionIngredients as Ingredients
from snapred.backend.dao.request.FarmFreshIngredients import FarmFreshIngredients
from snapred.backend.service.SousChef import SousChef
# for loading data
from snapred.backend.dao.ingredients.GroceryListItem import GroceryListItem
from snapred.backend.data.GroceryService import GroceryService

from snapred.meta.Config import Config, Resource
Config._config['cis_mode'] = False
Resource._resourcesPath = os.path.expanduser("~/SNAPRed/tests/resources/")

## USER INPUTS ###########################################################
VRun = 57473
VBRun = 57472
groupingScheme = "Column"
liteMode=True
calibrantSamplePath = "/SNS/SNAP/shared/Calibration/CalibrantSamples/Silicon_NIST_640D_001.json"
# File defining reduction parameters for this instrument state (Manually spec'ed for now)
sPrmFile = '/SNS/SNAP/shared/Calibration/Powder/04bd2c53f6bf6754/057514/SNAPcalibLog57514.lite.json' 
iPrmFile = '/SNS/SNAP/shared/Calibration/SNAPInstPrm.json'
idf = '/SNS/SNAP/shared/Calibration_old/SNAPLite.xml'
#############################Don't edit below here#########################

## PREP INGREDIENTS #######################################################
farmFresh = FarmFreshIngredients(
    runNumber=VRun,
    useLiteMode=liteMode,
    focusGroup={"name": groupingScheme, "definition": ""},
    calibrantSamplePath = calibrantSamplePath,
)
ingredients = SousChef().prepNormalizationIngredients(farmFresh)
###########################################################################

## FETCH GROCERIES ########################################################
clerk = GroceryListItem.builder()
clerk.neutron(VRun).useLiteMode(liteMode).add()
clerk.neutron(VBRun).useLiteMode(liteMode).add()
clerk.fromRun(VRun).grouping(groupingScheme).useLiteMode(liteMode).add()
groceries = GroceryService().fetchGroceryList(clerk.buildList())
###########################################################################

## RUN ALGORITHM ##########################################################
outputWS = "_test_raw_vanadium_final_output"
RawVanadiumCorrectionAlgorithm(
    InputWorkspace = groceries[0],
    BackgroundWorkspace = groceries[1],
    Ingredients = ingredients.json(),
    OutputWorkspace = outputWS,
)

ConvertUnits(
   InputWorkspace = outputWS,
   OutputWorkspace = outputWS,
   Target = 'dSpacing',
)  

DiffractionFocussing(
    InputWorkspace = outputWS,
    OutputWorkspace = '_test_focussed',
    GroupingWorkspace = groceries[2],
)
###########################################################################