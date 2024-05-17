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

from mantid.testing import assert_almost_equal as assert_wksp_almost_equal

groceryService = GroceryService()

#User input ###########################
runNumber = "46680" #"57482"
calibrantSamplePath = "SNS/SNAP/shared/Calibration/CalibrationSamples/Diamond_001.json"
peakThreshold = 0.01
isLite = True
Config._config["cis_mode"] = True
version="*"


### PREP INGREDIENTS ################

groups = LocalDataService().readGroupingMap(runNumber).getMap(isLite)

farmFresh = FarmFreshIngredients(
    runNumber=runNumber,
    useLiteMode=isLite,
    focusGroup=list(groups.values()),
    smoothingParameter=0.005,
    calibrantSamplePath=calibrantSamplePath,
    peakIntensityThresold=peakThreshold,
)
ingredients = SousChef().prepReductionIngredients(farmFresh)

# TODO: This probably needs to be a thing:
# ingredients.detectorPeaks = normalizationRecord.detectorPeaks


### FETCH GROCERIES ##################

clerk = GroceryListItem.builder()

for key, group in groups.items():
  clerk.fromRun(runNumber).grouping(group.name).useLiteMode(isLite).add()
groupingWorkspaces = GroceryService().fetchGroceryList(clerk.buildList())
# ...
clerk.name("inputWorkspace").neutron(runNumber).useLiteMode(isLite).add()
clerk.name("diffcalWorkspace").diffcal_table(runNumber).add()
clerk.name("normalizationWorkspace").normalization(runNumber).useLiteMode(isLite).add()

groceryDict = clerk.buildDict()
for key, value in groceryDict.items():
    print(f"\tKEY={key} : {value}")
groceries = GroceryService().fetchGroceryDict(
    groceryDict=groceryDict
)
groceries["groupWorkspaces"] = groupingWorkspaces

print(groceries.keys())
print(groceries["groupWorkspaces"])
print(groceries["groupingWorkspace"])


# NOTE: load the normalization the old way
# this will prove that the grocery service is able to load it for us now, 
# duplicated the previous behavior
localdataservice = LocalDataService()
normalizationRecord = localdataservice._getCurrentNormalizationRecord(runNumber, isLite)
print(localdataservice._constructNormalizationDataPath(runNumber, isLite, version))
version = normalizationRecord.version
normalizationPath = Path(localdataservice._constructNormalizationDataPath(runNumber, isLite, version))
normalizationWsName = wng.rawVanadium().runNumber(normalizationRecord.runNumber).build()
normalizationWs = groceryService.fetchWorkspace(str(normalizationPath / (normalizationWsName +f"_{wnvf.formatVersion(version=version, use_v_prefix=True)}" + ".nxs")), normalizationWsName)
assert_wksp_almost_equal(normalizationWs["workspace"], groceries["normalizationWorkspace"])

# run the recipe
Recipe().cook(ingredients, groceries)

