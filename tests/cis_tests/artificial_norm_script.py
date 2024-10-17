# Use this script to test Diffraction Calibration
from mantid.simpleapi import *
import matplotlib.pyplot as plt
import numpy as np
import json
from typing import List


## for creating ingredients
from snapred.backend.dao.request.FarmFreshIngredients import FarmFreshIngredients
from snapred.backend.dao.ingredients.ArtificialNormalizationIngredients import ArtificialNormalizationIngredients as FakeNormIngredients
from snapred.backend.service.SousChef import SousChef

## for loading data
from snapred.backend.dao.ingredients.GroceryListItem import GroceryListItem
from snapred.backend.data.GroceryService import GroceryService

## the code to test
from snapred.backend.recipe.algorithm.PixelDiffractionCalibration import PixelDiffCalRecipe as PixelRx
from snapred.backend.recipe.algorithm.GroupDiffractionCalibration import GroupDiffCalRecipe as GroupRx
from snapred.backend.recipe.algorithm.CreateArtificialNormalizationAlgo import CreateArtificialNormalizationAlgo as FakeNormAlgo
from snapred.backend.recipe.DiffractionCalibrationRecipe import DiffractionCalibrationRecipe as Recipe

# for running through service layer
from snapred.backend.service.CalibrationService import CalibrationService
from snapred.backend.dao.request.DiffractionCalibrationRequest import DiffractionCalibrationRequest

from snapred.meta.Config import Config

#User input ###########################
runNumber = "58882"
groupingScheme = "Column"
cifPath = "/SNS/users/dzj/Calibration_next/CalibrantSamples/cif/Silicon_NIST_640d.cif"
calibrantSamplePath = "/SNS/users/dzj/Calibration_next/CalibrantSamples/Silicon_NIST_640D_001.json"
peakThreshold = 0.05
offsetConvergenceLimit = 0.1
isLite = True
Config._config["cis_mode"] = False
#######################################

### PREP INGREDIENTS ################
farmFresh = FarmFreshIngredients(
    runNumber=runNumber,
    useLiteMode=isLite,
    focusGroups=[{"name": groupingScheme, "definition": ""}],
    cifPath=cifPath,
    calibrantSamplePath=calibrantSamplePath,
    convergenceThreshold=offsetConvergenceLimit,
    maxOffset=100.0,
)
ingredients = SousChef().prepDiffractionCalibrationIngredients(farmFresh)

### FETCH GROCERIES ##################

clerk = GroceryListItem.builder()
clerk.neutron(runNumber).useLiteMode(isLite).add()
clerk.fromRun(runNumber).grouping(groupingScheme).useLiteMode(isLite).add()
groceries = GroceryService().fetchGroceryList(clerk.buildList())

### RUN PIXEL CALIBRATION ##########
pixelRes = PixelRx().cook(ingredients, groceries)
assert False

### RUN GROUP CALIBRATION

DIFCprev = pixelRes.calibrationTable

outputWS = mtd.unique_name(prefix="output_")



groupAlgo = GroupAlgo()
groupAlgo.initialize()
groupAlgo.setPropertyValue("Ingredients", ingredients.json())
groupAlgo.setPropertyValue("InputWorkspace", groceries[0])
groupAlgo.setPropertyValue("GroupingWorkspace", groceries[1])
groupAlgo.setPropertyValue("PreviousCalibrationTable", DIFCprev)
groupAlgo.setPropertyValue("OutputWorkspace", outputWS)
groupAlgo.execute()

### PAUSE
"""
Stop here and examine the fits.
Make sure the diffraction focused TOF workspace looks as expected.
Make sure the offsets workspace has converged, the DIFCpd only fit pixels inside the group,
and that the fits match with the TOF diffraction focused workspace.
"""
assert False

def convertIngredients(ingredients):
    if hasattr(ingredients, "json"):  # If it's a Pydantic object, use the json() method
        return ingredients.json()
    elif isinstance(ingredients, dict):  # If it's already a dictionary, convert to JSON
        return json.dumps(ingredients)
    else:
        raise TypeError("The provided ingredients object is not compatible with JSON serialization")


fakeNormIngredients = FakeNormIngredients(
    peakWindowClippingSize=10,
    smoothingParameter=0.1,
    decreaseParameter=True,
    lss=True,
)

fakeNormIngredients_json = convertIngredients(fakeNormIngredients)

fakeNormAlgo = FakeNormAlgo()
fakeNormAlgo.initialize()
fakeNormAlgo.setPropertyValue("InputWorkspace", outputWS)
fakeNormAlgo.setPropertyValue("Ingredients", fakeNormIngredients_json)
fakeNormAlgo.setPropertyValue("OutputWorkspace", outputWS)
fakeNormAlgo.execute()

assert False

### This following script reflects the use of this new algo "CreateArtificialNormalizationAlgo" within Reduction
import json

## for creating ingredients
from snapred.backend.dao.request.FarmFreshIngredients import FarmFreshIngredients
from snapred.backend.service.SousChef import SousChef
from snapred.backend.data.LocalDataService import LocalDataService
from snapred.backend.dao.ingredients.ArtificialNormalizationIngredients import ArtificialNormalizationIngredients as FakeNormIngredients
## for loading data
from snapred.backend.dao.ingredients.GroceryListItem import GroceryListItem
from snapred.backend.data.GroceryService import GroceryService

## the code to test
from snapred.backend.recipe.ReductionRecipe import ReductionRecipe as Recipe

# for running through service layer
from snapred.backend.dao.request.ReductionRequest import ReductionRequest
from snapred.backend.service.ReductionService import ReductionService
from snapred.backend.recipe.algorithm.FocusSpectraAlgorithm import FocusSpectraAlgorithm as FocusSpec
from snapred.backend.recipe.algorithm.CreateArtificialNormalizationAlgo import CreateArtificialNormalizationAlgo as FakeNormAlgo

from snapred.meta.Config import Config
from pathlib import Path
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceNameGenerator as wng, ValueFormatter as wnvf

from mantid.testing import assert_almost_equal as assert_wksp_almost_equal
from mantid.simpleapi import ConvertToMatrixWorkspace

groceryService = GroceryService()

#User input ###########################
runNumber = "46680" #"57482"
isLite = True
Config._config["cis_mode"] = True
version=(1, None)
grouping = "Column"

### PREP INGREDIENTS ################

groups = LocalDataService().readGroupingMap(runNumber).getMap(isLite)

farmFresh = FarmFreshIngredients(
    runNumber=runNumber,
    versions=version,
    useLiteMode=isLite,
    focusGroups=list(groups.values()),
    timestamp=1726848143.8856316,
    keepUnfocused=True,
    convertUnitsTo="TOF",
)

ingredients = SousChef().prepReductionIngredients(farmFresh)

selectedFocusGroup = next(
    (fg for fg in farmFresh.focusGroups if fg.name == grouping), None
)

if selectedFocusGroup:
    print(f"Selected FocusGroup: {selectedFocusGroup}")
    
    updatedFarmFresh = FarmFreshIngredients(
        runNumber=farmFresh.runNumber,
        useLiteMode=farmFresh.useLiteMode,
        focusGroups=[selectedFocusGroup], 
    )
    
    pixelGroup = SousChef().prepPixelGroup(updatedFarmFresh)

# TODO: This probably needs to be a thing:
# ingredients.detectorPeaks = normalizationRecord.detectorPeaks


### FETCH GROCERIES ##################

clerk = GroceryListItem.builder()

for key, group in groups.items():
  clerk.fromRun(runNumber).grouping(group.name).useLiteMode(isLite).add()
groupingWorkspaces = GroceryService().fetchGroceryList(clerk.buildList())
# ...
clerk.name("inputWorkspace").neutron(runNumber).useLiteMode(isLite).add()
clerk.name("diffcalWorkspace").diffcal_table(runNumber, 1).useLiteMode(isLite).add()


groceries = GroceryService().fetchGroceryDict(
    groceryDict=clerk.buildDict()
)
groceries["groupingWorkspaces"] = groupingWorkspaces

rawWs = "tof_all_lite_copy1_046680"
groupingWs = f"SNAPLite_grouping__{grouping}_{runNumber}"

focusSpec =  FocusSpec()
focusSpec.initialize()
focusSpec.setPropertyValue("InputWorkspace", rawWs)
focusSpec.setPropertyValue("OutputWorkspace", rawWs)
focusSpec.setPropertyValue("GroupingWorkspace", groupingWs)
focusSpec.setPropertyValue("Ingredients", pixelGroup.json())
focusSpec.setProperty("RebinOutput", False)
focusSpec.execute()

rawWs = ConvertToMatrixWorkspace(rawWs)

def convertIngredients(ingredients):
    if hasattr(ingredients, "json"):  # If it's a Pydantic object, use the json() method
        return ingredients.json()
    elif isinstance(ingredients, dict):  # If it's already a dictionary, convert to JSON
        return json.dumps(ingredients)
    else:
        raise TypeError("The provided ingredients object is not compatible with JSON serialization")


fakeNormIngredients = FakeNormIngredients(
    peakWindowClippingSize=10,
    smoothingParameter=0.1,
    decreaseParameter=True,
    lss=True,
)

fakeNormIngredients_json = convertIngredients(fakeNormIngredients)

fakeNormAlgo = FakeNormAlgo()
fakeNormAlgo.initialize()
fakeNormAlgo.setProperty("InputWorkspace", rawWs)
fakeNormAlgo.setPropertyValue("Ingredients", fakeNormIngredients_json)
fakeNormAlgo.setProperty("OutputWorkspace", "artificial_Norm_Ws")
fakeNormAlgo.execute()

assert False