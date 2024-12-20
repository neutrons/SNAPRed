## This is for testing EWM 2357
#   https://ornlrse.clm.ibmcloud.com/ccm/web/projects/Neutron%20Data%20Project%20%28Change%20Management%29#action=com.ibm.team.workitem.viewWorkItem&id=2357
# Should verify that peaks are removed from the spetrum and there is a smooth fit over the removed parts


import snapred.backend.recipe.algorithm
from mantid.simpleapi import Rebin, SmoothDataExcludingPeaksAlgo, ConvertUnits, DiffractionFocussing

# try to make the logger shutup
from snapred.backend.log.logger import snapredLogger
snapredLogger._level = 60

# for creating the ingredients
from snapred.backend.dao.request.FarmFreshIngredients import FarmFreshIngredients
from snapred.backend.service.SousChef import SousChef

#for loading workspaces
from snapred.backend.dao.ingredients.GroceryListItem import GroceryListItem
from snapred.backend.data.GroceryService import GroceryService

from snapred.meta.pointer import create_pointer

#User inputs ###########################
runNumber = "58882" #58409
isLite = True
groupingScheme = "Column"
calibrantSamplePath = "Silicon_NIST_640D_001.json"
smoothingParameter = 0.05
#######################################

## PREP INGREDIENTS
farmFresh = FarmFreshIngredients(
  runNumber = runNumber,
  useLiteMode=isLite,
  focusGroups=[{"name": groupingScheme, "definition": ""}],
  calibrantSamplePath=calibrantSamplePath,
)
peaks = SousChef().prepDetectorPeaks(farmFresh)

## FETCH GROCERIES
simpleList = GroceryListItem.builder().neutron(runNumber).useLiteMode(isLite).buildList()

clerk = GroceryListItem.builder()
clerk.name("inputWorkspace").neutron(runNumber).useLiteMode(isLite).add()
clerk.name("groupingWorkspace").fromRun(runNumber).grouping(groupingScheme).useLiteMode(isLite).add()
groceries = GroceryService().fetchGroceryDict(clerk.buildDict())

inputWS = groceries["inputWorkspace"]
focusWS = groceries["groupingWorkspace"]

## PREPARE 
# data must be in units of d-spacing
# we must convert the event data to histogram data
ConvertUnits(
    InputWorkspace = inputWS,
    OutputWorkspace="in_ws",
    Target="dSpacing",
)
DiffractionFocussing(
    InputWorkspace="in_ws",
    GroupingWorkspace=focusWS,
    OutputWorkspace="in_ws",
    PreserveEvents=False, # will convert to histogram
)

## RUN ALGORITHM

assert SmoothDataExcludingPeaksAlgo(
  InputWorkspace = "in_ws",
  DetectorPeaks = create_pointer(peaks),
  SmoothingParameter = smoothingParameter,
  OutputWorkspace = "out_ws",
)
