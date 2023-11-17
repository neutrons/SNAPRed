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
from snapred.backend.service.CalibrationService import CalibrationService
from snapred.backend.dao.request.DiffractionCalibrationRequest import DiffractionCalibrationRequest

from snapred.meta.Config import Config

#User input ###########################
runNumber = "57514"
cifPath = "/SNS/SNAP/shared/Calibration/CalibrantSamples/Silicon_NIST_640d.cif"
peakThreshold = 0.005
offsetConvergenceLimit = 0.01
isLite = True
Config._config["cis_mode"] = False
#######################################

### CREATE INGREDIENTS ################
runConfig = RunConfig(
    runNumber=runNumber,
    IPTS=GetIPTS(RunNumber=runNumber,Instrument='SNAP'), 
    isLite=isLite,
)
dataFactoryService = DataFactoryService()
focusGroup=dataFactoryService.getFocusGroups(runNumber)[0] #column
calibration = dataFactoryService.getCalibrationState(runNumber)

calibrationService = CalibrationService()
pixelGroupingParameters = calibrationService.retrievePixelGroupingParams(runNumber)

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
        runConfig = runConfig,
        loader="LoadEventNexus",
    ),
    GroceryListItem(
        workspaceType="grouping",
        isLite=isLite,
        groupingScheme="Column",
        instrumentPropertySource="InstrumentDonor",
        instrumentSource="prev",
    ),
]
fetchRx = FetchRx()
data = fetchRx.executeRecipe(groceryList)
print(json.dumps(data, indent=2))

### RUN PIXEL CALIBRATION ##########
pixelAlgo = PixelAlgo()
pixelAlgo.initialize()
pixelAlgo.setPropertyValue("Ingredients", ingredients.json())
pixelAlgo.setPropertyValue("InputWorkspace", data["workspaces"][0])
pixelAlgo.setPropertyValue("GroupingWorkspace", data["workspaces"][1])
pixelAlgo.execute()

median = json.loads(pixelAlgo.getPropertyValue("data"))["medianOffset"]
print(median)

while median > offsetConvergenceLimit:
    pixelAlgo.execute()
    median = json.loads(pixelAlgo.getPropertyValue("data"))["medianOffset"]

### RUN GROUP CALIBRATION
groupAlgo = GroupAlgo()
groupAlgo.initialize()
groupAlgo.setPropertyValue("Ingredients", ingredients.json())
groupAlgo.setPropertyValue("InputWorkspace", data["workspaces"][0])
groupAlgo.setPropertyValue("GroupingWorkspace", data["workspaces"][1])
groupAlgo.setPropertyValue("PreviousCalibrationTable", "_DIFC_57514")
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
rx = Recipe()
groceries = {
    "inputWorkspace": data["workspaces"][0],
    "groupingWorkspace": data["workspaces"][1],
}
rx.executeRecipe(ingredients, groceries)


### PAUSE
"""
Stop here and make sure everything still looks good.
"""
assert False

### CALL CALIBRATION SERVICE

diffcalRequest = DiffractionCalibrationRequest(
    runNumber = runNumber,
    cifPath = cifPath,
    useLiteMode = isLite,
    focusGroupPath = ingredients.focusGroup.definition,
    convergenceThreshold = offsetConvergenceLimit,
    peakIntensityThreshold = peakThreshold,
)

calibrationService = CalibrationService()
res = calibrationService.diffractionCalibration(diffcalRequest)
print(json.dumps(res,indent=2))
assert False