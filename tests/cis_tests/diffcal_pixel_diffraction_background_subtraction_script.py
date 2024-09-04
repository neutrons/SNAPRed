# Use this script to test Pixel Diffraction Background Subtraction
from mantid.simpleapi import *
import matplotlib.pyplot as plt
import numpy as np
import json
from typing import List


## for creating ingredients
from snapred.backend.dao.request.FarmFreshIngredients import FarmFreshIngredients
from snapred.backend.service.SousChef import SousChef

## for loading data
from snapred.backend.dao.ingredients.GroceryListItem import GroceryListItem
from snapred.backend.data.GroceryService import GroceryService

## the code to test
from snapred.backend.recipe.algorithm.PixelDiffractionCalibration import PixelDiffractionCalibration as PixelAlgo
from snapred.backend.recipe.algorithm.GroupDiffractionCalibration import GroupDiffractionCalibration as GroupAlgo
from snapred.backend.recipe.DiffractionCalibrationRecipe import DiffractionCalibrationRecipe as Recipe

# for running through service layer
from snapred.backend.service.CalibrationService import CalibrationService
from snapred.backend.dao.request.DiffractionCalibrationRequest import DiffractionCalibrationRequest

from snapred.meta.Config import Config

#User input ###########################
runNumber = "58882"
groupingScheme = "Column"
cifPath = "/SNS/SNAP/shared/Calibration/CalibrantSamples/Silicon_NIST_640d.cif"
calibrantSamplePath = "SNS/SNAP/shared/Calibration/CalibrationSamples/Silicon_NIST_640D_001.json"
peakThreshold = 0.05
offsetConvergenceLimit = 0.1
isLite = True
removeBackground = False #NOTE: True == don't remove background // False == remove background
Config._config["cis_mode"] = True
Config._config["diffraction.smoothingParameter"] = 0.01  #This is the smoothing parameter to be set.
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

# HERE IS THE BACKGROUND REMOVAL TOGGLE!
ingredients.removeBackground = removeBackground 

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
