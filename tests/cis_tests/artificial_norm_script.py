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
from snapred.backend.recipe.algorithm.PixelDiffractionCalibration import PixelDiffractionCalibration as PixelAlgo
from snapred.backend.recipe.algorithm.GroupDiffractionCalibration import GroupDiffractionCalibration as GroupAlgo
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
pixelAlgo = PixelAlgo()
pixelAlgo.initialize()
pixelAlgo.setPropertyValue("Ingredients", ingredients.json())
pixelAlgo.setPropertyValue("InputWorkspace",groceries[0])
pixelAlgo.setPropertyValue("GroupingWorkspace", groceries[1])
pixelAlgo.execute()

assert False

median = json.loads(pixelAlgo.getPropertyValue("data"))["medianOffset"]
print(median)

count = 0
while median > offsetConvergenceLimit or count < 5:
    pixelAlgo.execute()
    median = json.loads(pixelAlgo.getPropertyValue("data"))["medianOffset"]
    count += 1
    
### RUN GROUP CALIBRATION

DIFCprev = pixelAlgo.getPropertyValue("CalibrationTable")

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
