"""
NOTE this is dead code
The algorithm it tests has been deleted.
This is being retained for possible future testing of other parts of normalization
"""
raise NotImplementedError("The algorithm tested by this script no longer exists.")

import snapred.backend.recipe.algorithm
from mantid.simpleapi import *
import matplotlib.pyplot as plt
import numpy as np

import json

## For creating ingredients
from snapred.backend.dao.request.FarmFreshIngredients import FarkFreshIngredients
from snapred.backend.service.SousChef import SousChef

# for retrieving data
from snapred.backend.data.GroceryService import GroceryService
from snapred.backend.dao.ingredients.GroceryListItem import GroceryListItem

from snapred.meta.Config import Config

#User input ###############################################################################################
isLite = True
runNumber = "58810"
backgroundRunNumber = "58813"
samplePath = "/SNS/SNAP/shared/Calibration_dynamic/CalibrantSample/Silicon_NIST_640D_001.json"
cifPath = "/SNS/SNAP/shared/Calibration/CalibrantSample/Silicon_NIST_640d.cif"
groupPath = "/SNS/SNAP/shared/Calibration/Powder/PixelGroupingDefinitions/SNAPFocGrp_Column.lite.xml"
calibrationWorkspace = "/SNS/users/dzj/Desktop/58810_calibration_ws.nxs" # need to create this using diffc_test.py with run number 58810
smoothingParam = 0.50
groupingScheme = "Column"
calibrationStatePath = "/SNS/SNAP/shared/Calibration/Powder/04bd2c53f6bf6754/57514/v_1/CalibrationParameters.json" # need to change this!!!!!!
###########################################################################################################
def getCalibrantSample(samplePath):
    with open(samplePath, 'r') as file:
        sampleJson = json.load(file)
    del sampleJson["material"]["packingFraction"]
    for atom in sampleJson["crystallography"]["atoms"]:
        atom["symbol"] = atom.pop("atom_type")
        atom["coordinates"] = atom.pop("atom_coordinates")
        atom["siteOccupationFactor"] = atom.pop("site_occupation_factor")
    sample = CalibrantSample.model_validate_json(json.dumps(sampleJson))
    return sample
###########################################################################################################

DFS = DataFactoryService()
instrumentState = DFS.getCalibrationState(runNumber).instrumentState

### PREP INGREDIENTS ################
farmFresh = FarmFreshIngredients(
    runNumber=runNumber,
    useLiteMode=isLite,
    focusGroups=[{"name": groupingScheme, "definition": groupPath}],
    cifPath=cifPath,
    calibrantSamplePath=samplePath,
)
ingredients = SousChef().getNormalizationIngredients(farmFresh)

### FETCH GROCERIES ##################
clerk = GroceryListItem.builder()
clerk.name("inputWorkspace").neutron(runNumber).useLiteMode(isLite).add()
clerk.name("backgroundWorkspace").neutron(backgroundRunNumber).useLiteMode(isLite).add()
clerk.name("groupingWorkspace").fromRun(runNumber).grouping(groupingScheme).useLiteMode(isLite).add()
groceries = GroceryService().fetchGroceryList(clerk.buildList())
