## This is for testing EWM 2357
#   https://ornlrse.clm.ibmcloud.com/ccm/web/projects/Neutron%20Data%20Project%20%28Change%20Management%29#action=com.ibm.team.workitem.viewWorkItem&id=2357
# Should verify that peaks are removed from the spetrum and there is a smooth fit over the removed parts

from mantid.simpleapi import *
import matplotlib.pyplot as plt
import numpy as np
import json

# try to make the logger shutup
from snapred.backend.log.logger import snapredLogger
snapredLogger._level = 60

# for creating the ingredients
from snapred.backend.service.CrystallographicInfoService import CrystallographicInfoService
from snapred.backend.service.DiffractionCalibrationService import DiffractionCalibrationService

# the algorithm to test (and its ingredients)
from snapred.backend.dao.ingredients import SmoothDataExcludingPeaksIngredients
from snapred.backend.recipe.algorithm.SmoothDataExcludingPeaksAlgo import SmoothDataExcludingPeaksAlgo

#for loading workspaces
from snapred.backend.dao.ingredients.GroceryListItem import GroceryListItem
from snapred.backend.recipe.FetchGroceriesRecipe import FetchGroceriesRecipe as FetchRx

#User inputs ###########################
runNumber = "58882" #58409
cifPath = "/SNS/SNAP/shared/Calibration/CalibrantSamples/Silicon_NIST_640d.cif"
#######################################


## CREATE INGREDIENTS
diffractionCalibrationService = DiffractionCalibrationService()
pixelGroupingParameters = diffractionCalibrationService.retrievePixelGroupingParams(runNumber)
calibration = dataFactoryService.getCalibrationState(runNumber)
calibration.instrumentState.pixelGroupingInstrumentParameters = pixelGroupingParameters[0]

ingredients = SmoothDataExcludingPeaksIngredients(
    instrumentState=calibration.instrumentState, 
    crystalInfo=CrystallographicInfoService().ingest(cifPath)['crystalInfo'], 
    smoothingParameter=0.05,
)


## FETCH GROCERIES
grocery = FetchRx().fetchCleanNexusData(GroceryListItem.makeLiteNexusItem(runNumber))["workspace"]
# we must convert the event data to histogram data
# this rebin step will accomplish that, due to PreserveEvents = False
Rebin(
    InputWorkspace = grocery,
    OutputWorkspace = grocery,
    Params = (1,-0.01,1667.7),
    PreserveEvents = False,
)

## RUN ALGORITHM
algo = SmoothDataExcludingPeaksAlgo()
algo.initialize()
algo.setProperty("Ingredients", ingredients.json())
algo.setProperty("InputWorkspace", grocery)
algo.execute()
