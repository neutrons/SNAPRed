# import mantid algorithms, numpy and matplotlib
import snapred.backend.recipe.algorithm
from mantid.simpleapi import DiffractionFocussing, Rebin, RemoveSmoothedBackground
import matplotlib.pyplot as plt
import numpy as np
import json
import time
## for creating ingredients
from snapred.backend.dao.request.FarmFreshIngredients import FarmFreshIngredients
from snapred.backend.service.SousChef import SousChef

## for loading data
from snapred.backend.dao.ingredients.GroceryListItem import GroceryListItem
from snapred.backend.data.GroceryService import GroceryService

from snapred.meta.Config import Config
from snapred.meta.pointer import create_pointer, access_pointer
from snapred.meta.redantic import list_to_raw

#User input ###########################
runNumber = "58882"
groupingScheme = "Column"
calibrantSamplePath = "Silicon_NIST_640D_001.json"
peakThreshold = 0.05
offsetConvergenceLimit = 0.1
isLite = True
Config._Config["cis_mode"]["enabled"] = True
#######################################

### PREP INGREDIENTS ################

farmFresh = FarmFreshIngredients(
    runNumber=runNumber,
    useLiteMode=isLite,
    focusGroups=[{"name": groupingScheme, "definition": ""}],
    calibrantSamplePath=calibrantSamplePath,
    peakIntensityThreshold=peakThreshold,
    convergenceThreshold=offsetConvergenceLimit,
    maxOffset=100.0,
)
pixelGroup = SousChef().prepPixelGroup(farmFresh)
detectorPeaks = SousChef().prepDetectorPeaks(farmFresh)

peaks = "peaks"

### FETCH GROCERIES ##################

clerk = GroceryListItem.builder()
clerk.name("inputWorkspace").neutron(runNumber).useLiteMode(isLite).add()
clerk.name("groupingWorkspace").fromRun(runNumber).grouping(groupingScheme).useLiteMode(isLite).add()
groceries = GroceryService().fetchGroceryDict(clerk.buildDict())

## UNBAG GROCERIES

inputWorkspace = groceries["inputWorkspace"]
focusWorkspace = groceries["groupingWorkspace"]
Rebin(
    InputWorkspace=inputWorkspace,
    OutputWorkspace=inputWorkspace,
    Params=pixelGroup.timeOfFlight.params,
)

### REMOVE EVENT BACKGROUND BY SMOOTHING ##

start = time.time()
RemoveSmoothedBackground(
    InputWorkspace=inputWorkspace,
    GroupingWorkspace=focusWorkspace,
    OutputWorkspace=peaks,
    DetectorPeaks = create_pointer(detectorPeaks),
    SmoothingParameter=0.5,
)
end = time.time()
print(f"TIME FOR ALGO = {end-start}")

DiffractionFocussing(
    InputWorkspace="peaks_extractDSP_before",
    OutputWorkspace="peaks_extractDSP_before_foc",
    GroupingWorkspace=focusWorkspace,
)
DiffractionFocussing(
    InputWorkspace="peaks_extractDSP_after",
    OutputWorkspace="peaks_extractDSP_after_foc",
    GroupingWorkspace=focusWorkspace,
)