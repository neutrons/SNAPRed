# Use this script to test Diffraction Calibration
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
from snapred.backend.recipe.PixelDiffCalRecipe import PixelDiffCalRecipe as PixelDiffCalRx
from snapred.backend.recipe.GroupDiffCalRecipe import GroupDiffCalRecipe as GroupDiffCalRx

# for running through service layer
from snapred.backend.service.CalibrationService import CalibrationService
from snapred.backend.dao.request.DiffractionCalibrationRequest import DiffractionCalibrationRequest

from snapred.meta.Config import Config

#User input ###########################
runNumber = "58882"
groupingScheme = "Column"
cifPath = "/SNS/SNAP/shared/Calibration/CalibrantSamples/Silicon_NIST_640d.cif"
calibrantSamplePath = "Silicon_NIST_640D_001.json"
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
    peakIntensityThreshold=peakThreshold,
    convergenceThreshold=offsetConvergenceLimit,
    maxOffset=100.0,
)
ingredients = SousChef().prepDiffractionCalibrationIngredients(farmFresh)

### FETCH GROCERIES ##################

clerk = GroceryListItem.builder()
clerk.name("inputWorkspace").neutron(runNumber).useLiteMode(isLite).add()
clerk.name("groupingWorkspace").fromRun(runNumber).grouping(groupingScheme).useLiteMode(isLite).add()
groceries = GroceryService().fetchGroceryDict(
    clerk.buildDict(),
    outputWorkspace="_out_",
    diagnosticWorkspace="_diag",
    maskWorkspace="_mask_",
    calibrationTable="_DIFC_",    
)

### RUN PIXEL CALIBRATION ##########

pixelRes = PixelDiffCalRx().cook(ingredients, groceries)
    
### RUN GROUP CALIBRATION

DIFCprev = pixelRes.calibrationTable

groupGroceries = groceries.copy()
groupGroceries["previousCalibration"] = DIFCprev
groupGroceries["calibrationTable"] = DIFCprev
groupRes = GroupDiffCalRx().cook(ingredients, groceries)

### PAUSE
"""
Stop here and examine the fits.
Make sure the diffraction focused TOF workspace looks as expected.
Make sure the offsets workspace has converged, the DIFCpd only fit pixels inside the group,
and that the fits match with the TOF diffraction focused workspace.
"""
assert False

### CALL CALIBRATION SERVICE
diffcalRequest = DiffractionCalibrationRequest(
    runNumber = runNumber,
    calibrantSamplePath = calibrantSamplePath,
    useLiteMode = isLite,
    focusGroup = {"name": groupingScheme, "definition": ""},
    convergenceThreshold = offsetConvergenceLimit,
    removeBackground=False,
)

calibrationService = CalibrationService()
res = calibrationService.diffractionCalibration(diffcalRequest)
print(json.dumps(res,indent=2))
assert False
