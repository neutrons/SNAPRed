## This is for testing EWM 2357
#   https://ornlrse.clm.ibmcloud.com/ccm/web/projects/Neutron%20Data%20Project%20%28Change%20Management%29#action=com.ibm.team.workitem.viewWorkItem&id=2357
# Should verify that peaks are removed from the spetrum and there is a smooth fit over the removed parts


import snapred.backend.recipe.algorithm
from mantid.simpleapi import Rebin, SmoothDataExcludingPeaksAlgo

# try to make the logger shutup
from snapred.backend.log.logger import snapredLogger
snapredLogger._level = 60

# for creating the ingredients
from snapred.backend.dao.request.FarmFreshIngredients import FarmFreshIngredients
from snapred.backend.service.SousChef import SousChef

#for loading workspaces
from snapred.backend.dao.ingredients.GroceryListItem import GroceryListItem
from snapred.backend.data.GroceryService import GroceryService

from snapred.meta.redantic import list_to_raw

#User inputs ###########################
runNumber = "58882" #58409
isLite = True
groupingScheme = "Column"
cifPath = "/SNS/SNAP/shared/Calibration/CalibrantSample/Silicon_NIST_640d.cif"
smoothingParameter = 0.05
#######################################

## PREP INGREDIENTS
farmFresh = FarmFreshIngredients(
  runNumber = runNumber,
  useLiteMode=isLite,
  focusGroups=[{"name": groupingScheme, "definition": ""}],
  cifPath=cifPath,
)
peaks = SousChef().prepDetectorPeaks(farmFresh)

## FETCH GROCERIES
simpleList = GroceryListItem.builder().neutron(runNumber).useLiteMode(isLite).buildList()
grocery = GroceryService().fetchGroceryList(simpleList)[0]
# we must convert the event data to histogram data
# this rebin step will accomplish that, due to PreserveEvents = False
Rebin(
    InputWorkspace = grocery,
    OutputWorkspace = grocery,
    Params = (1,-0.01,1667.7),
    PreserveEvents = False,
)

## RUN ALGORITHM
assert SmoothDataExcludingPeaksAlgo(
  InputWorkspace = grocery,
  DetectorPeaks = list_to_raw(peaks),
  SmoothingParameter = smoothingParameter,
  OutputWorkspace = "out_ws",
)
