# Use this script to test Diffraction Calibration
from mantid.simpleapi import *
import matplotlib.pyplot as plt
import numpy as np
import json
from pydantic import parse_raw_as
from typing import List

from snapred.backend.recipe.algorithm.PixelDiffractionCalibration import PixelDiffractionCalibration as PixelAlgo
from snapred.backend.recipe.algorithm.GroupDiffractionCalibration import GroupDiffractionCalibration as GroupAlgo
from snapred.backend.recipe.DiffractionCalibrationRecipe import DiffractionCalibrationRecipe as Recipe
from snapred.backend.recipe.algorithm.DetectorPeakPredictor import DetectorPeakPredictor
from snapred.backend.recipe.algorithm.PurgeOverlappingPeaksAlgorithm import PurgeOverlappingPeaksAlgorithm
from snapred.backend.recipe.FetchGroceriesRecipe import FetchGroceriesRecipe as FetchRx
from snapred.backend.dao.ingredients.GroceryListItem import GroceryListItem
from snapred.backend.dao.ingredients import DiffractionCalibrationIngredients
from snapred.backend.dao import RunConfig, DetectorPeak, GroupPeakList
from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.data.DataFactoryService import DataFactoryService
from snapred.backend.service.CrystallographicInfoService import CrystallographicInfoService
from snapred.backend.service.DiffractionDiffractionCalibrationService import DiffractionCalibrationService
from snapred.backend.dao.request.DiffractionCalibrationRequest import DiffractionCalibrationRequest

from snapred.meta.Config import Config

#User input ###########################
runNumber = "58882"
cifPath = "/SNS/SNAP/shared/Calibration/CalibrantSamples/Silicon_NIST_640d.cif"
peakThreshold = 0.05
offsetConvergenceLimit = 0.1
isLite = True
Config._config["cis_mode"] = True
#######################################

### CREATE INGREDIENTS ################
runConfig = RunConfig(
    runNumber=runNumber,
    IPTS=GetIPTS(RunNumber=runNumber,Instrument='SNAP'), 
    useLiteMode=isLite,
)
dataFactoryService = DataFactoryService()
focusGroup=dataFactoryService.getFocusGroups(runNumber)[0] #column
calibration = dataFactoryService.getCalibrationState(runNumber)

DiffractionCalibrationService = DiffractionCalibrationService()
pixelGroupingParameters = DiffractionCalibrationService.retrievePixelGroupingParams(runNumber)

instrumentState = calibration.instrumentState
calPath = instrumentState.instrumentConfig.calibrationDirectory
instrumentState.pixelGroupingInstrumentParameters = pixelGroupingParameters[0]

crystalInfoDict = CrystallographicInfoService().ingest(cifPath)

detectorAlgo = PurgeOverlappingPeaksAlgorithm()
detectorAlgo.initialize()
detectorAlgo.setProperty("InstrumentState", instrumentState.json())
detectorAlgo.setProperty("CrystalInfo", crystalInfoDict["crystalInfo"].json())
detectorAlgo.setProperty("PeakIntensityFractionThreshold", peakThreshold)
detectorAlgo.execute()
peakList = detectorAlgo.getProperty("OutputPeakMap").value
peakList = parse_raw_as(List[GroupPeakList], peakList)

ingredients = DiffractionCalibrationIngredients(
    runConfig=runConfig,
    instrumentState=instrumentState,
    focusGroup=focusGroup,
    groupedPeakLists=peakList,
    calPath=calPath,
    convergenceThreshold=offsetConvergenceLimit,
    maxOffset = 100.0,
)

### FETCH GROCERIES ##################
groceryList = [
    GroceryListItem(
        workspaceType="nexus",
        useLiteMode=isLite,
        runNumber=runNumber,
    ),
    GroceryListItem(
        workspaceType="grouping",
        useLiteMode=isLite,
        groupingScheme="Column",
        instrumentPropertySource="InstrumentDonor",
        instrumentSource="prev",
    ),
]
fetchRx = FetchRx()
# fetchRx._loadedRuns[("58882",False)] = 1 # uncomment this line if the raw file already exists
groceries = fetchRx.executeRecipe(groceryList)["workspaces"]
print(json.dumps(groceries, indent=2))

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

while median > offsetConvergenceLimit:
    pixelAlgo.execute()
    median = json.loads(pixelAlgo.getPropertyValue("data"))["medianOffset"]

### RUN GROUP CALIBRATION

DIFCprev = pixelAlgo.getPropertyValue("CalibrationTable")

groupAlgo = GroupAlgo()
groupAlgo.initialize()
groupAlgo.setPropertyValue("Ingredients", ingredients.json())
groupAlgo.setPropertyValue("InputWorkspace", groceries[0])
groupAlgo.setPropertyValue("GroupingWorkspace", groceries[1])
groupAlgo.setPropertyValue("PreviousCalibrationTable", DIFCprev)
groupAlgo.execute()

### PAUSE
"""
Stop here and examine the fits.
Make sure the diffraction focused TOF workspace looks as expected.
Make sure the offsets workspace has converged, the DIFCpd only fit pixels inside the group,
and that the fits match with the TOF diffraction focused workspace.
"""
assert False


### RUN RECIPE
from unittest import mock
fetchRx = FetchRx()
groceries = fetchRx.executeRecipe(groceryList)["workspaces"]
rx = Recipe()
groceries = {
    "inputWorkspace": groceries[0],
    "groupingWorkspace": groceries[1],
}
with mock.patch.object(Recipe, "restockShelves"):
    rx.executeRecipe(ingredients, groceries)


### PAUSE
"""
Stop here and make sure everything still looks good.
"""
assert False

### CALL CALIBRATION SERVICE
from unittest import mock
diffcalRequest = DiffractionCalibrationRequest(
    runNumber = runNumber,
    cifPath = cifPath,
    useLiteMode = isLite,
    focusGroupPath = ingredients.focusGroup.definition,
    convergenceThreshold = offsetConvergenceLimit,
    peakIntensityThreshold = peakThreshold,
)

DiffractionCalibrationService = DiffractionCalibrationService()
with mock.patch.object(Recipe, "restockShelves"):
    res = DiffractionCalibrationService.diffractionCalibration(diffcalRequest)
print(json.dumps(res,indent=2))
assert False