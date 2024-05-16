## for creating ingredients
from snapred.backend.dao.request.FarmFreshIngredients import FarmFreshIngredients
from snapred.backend.service.SousChef import SousChef
from snapred.backend.data.LocalDataService import LocalDataService
## for loading data
from snapred.backend.dao.ingredients.GroceryListItem import GroceryListItem
from snapred.backend.data.GroceryService import GroceryService

## the code to test
from snapred.backend.recipe.algorithm.PixelDiffractionCalibration import PixelDiffractionCalibration as PixelAlgo
from snapred.backend.recipe.algorithm.GroupDiffractionCalibration import GroupDiffractionCalibration as GroupAlgo
from snapred.backend.recipe.ReductionRecipe import ReductionRecipe as Recipe

# for running through service layer
from snapred.backend.service.CalibrationService import CalibrationService
from snapred.backend.dao.request.DiffractionCalibrationRequest import DiffractionCalibrationRequest
    
from snapred.meta.Config import Config
from pathlib import Path
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceNameGenerator as wng, ValueFormatter as wnvf

groceryService = GroceryService()

#User input ###########################
runNumber = "57482"
calibrantSamplePath = "SNS/SNAP/shared/Calibration/CalibrationSamples/vanadium_cylinder_001.json"
peakThreshold = 0.01
isLite = True
Config._config["cis_mode"] = True
version="*"

localdataservice = LocalDataService()

normalizationRecord = localdataservice._getCurrentNormalizationRecord(runNumber, isLite)
print(localdataservice._constructNormalizationDataPath(runNumber, isLite, version))
version = normalizationRecord.version

normalizationPath = Path(localdataservice._constructNormalizationDataPath(runNumber, isLite, version))
normalizationWsName = wng.rawVanadium().runNumber(normalizationRecord.runNumber).build()
# need to get all groupings for a given state
groups = LocalDataService().readStateConfig(runNumber, isLite).groupingMap.liteFocusGroups


### PREP INGREDIENTS ################
farmFresh = FarmFreshIngredients(
    runNumber=runNumber,
    useLiteMode=isLite,
    focusGroup=groups,
    smoothingParameter=normalizationRecord.smoothingParameter,
    calibrantSamplePath=calibrantSamplePath,
    peakIntensityThresold=peakThreshold,
)
peaks = SousChef().prepManyDetectorPeaks(farmFresh)

ingredients = SousChef().prepReductionIngredients(farmFresh)

# TODO: This probably needs to be a thing:
# ingredients.detectorPeaks = normalizationRecord.detectorPeaks



### FETCH GROCERIES ##################

clerk = GroceryListItem.builder()


for group in groups:
  clerk.fromRun(runNumber).grouping(group.name).useLiteMode(isLite).add()
groupingWorkspaces = GroceryService().fetchGroceryList(clerk.buildList())
# ...
clerk.name("inputWorkspace").neutron(runNumber).useLiteMode(isLite).add()
clerk.name("diffcalWorkspace").diffcal_table(runNumber).add()

groceries = GroceryService().fetchGroceryDict(
    groceryDict=clerk.buildDict()
)
groceries["groupWorkspaces"] = groupingWorkspaces


# TODO: Need a way to load normalization workspaces
normalizationWs = groceryService.fetchWorkspace(str(normalizationPath / (normalizationWsName +f"_{wnvf.formatVersion(version=version, use_v_prefix=True)}" + ".nxs")), normalizationWsName)

groceries["normalizationWorkspace"] = normalizationWs["workspace"]

Recipe().cook(ingredients, groceries)