## for creating ingredients
from snapred.backend.dao.request.FarmFreshIngredients import FarmFreshIngredients
from snapred.backend.service.SousChef import SousChef
from snapred.backend.data.LocalDataService import LocalDataService
## for loading data
from snapred.backend.dao.ingredients.GroceryListItem import GroceryListItem
from snapred.backend.data.GroceryService import GroceryService

## the code to test
from snapred.backend.recipe.ReductionRecipe import ReductionRecipe as Recipe

# for running through service layer
from snapred.backend.dao.request.ReductionRequest import ReductionRequest
from snapred.backend.service.ReductionService import ReductionService

from snapred.meta.Config import Config
from pathlib import Path
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceNameGenerator as wng, ValueFormatter as wnvf

from mantid.testing import assert_almost_equal as assert_wksp_almost_equal

groceryService = GroceryService()

#User input ###########################
runNumber = "46680" #"57482"
isLite = True
Config._config["cis_mode"] = True
version=None


### PREP INGREDIENTS ################

groups = LocalDataService().readGroupingMap(runNumber).getMap(isLite)

farmFresh = FarmFreshIngredients(
    runNumber=runNumber,
    useLiteMode=isLite,
    focusGroup=list(groups.values()),
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

groceries = GroceryService().fetchGroceryDict(
    groceryDict=clerk.buildDict()
)
groceries["groupingWorkspaces"] = groupingWorkspaces

# NOTE: load the normalization the old way
# this will prove that the grocery service is able to load it for us now, 
# duplicated the previous behavior
localdataservice = LocalDataService()
normalizationRecord = localdataservice.readNormalizationRecord(runNumber, isLite)
version = normalizationRecord.version
normalizationPath = Path(localdataservice._constructNormalizationDataPath(runNumber, isLite, version))
normalizationWsName = wng.rawVanadium().runNumber(normalizationRecord.runNumber).build()
normalizationWs = groceryService.fetchWorkspace(str(normalizationPath / (normalizationWsName +f"_{wnvf.formatVersion(version=version, use_v_prefix=True)}" + ".nxs")), normalizationWsName)
assert_wksp_almost_equal(normalizationWs["workspace"], groceries["normalizationWorkspace"])

# run the recipe
Recipe().cook(ingredients, groceries)

assert False

### TEST THROUGH THE SERVICE LAYER ###

request = ReductionRequest(
    runNumber = runNumber,
    useLiteMode = isLite,
)

service = ReductionService()

grouping2 = service.fetchReductionGroupings(request)
request.focusGroup = grouping2["focusGroups"]
assert grouping2["groupingWorkspaces"] == groupingWorkspaces

ingredients2 = service.prepReductionIngredients(request)
assert ingredients == ingredients2

groceries2 = service.fetchReductionGroceries(request)
groceries2["groupingWorkspaces"] = grouping2["groupingWorkspaces"]

# NOTE the groceries will have the "copyX" keyword in them
# indicating how many times they have been loaded.  
# Apart from that, the two lists should be identical.
import re
for key in groceries.keys():
    if "copy" in groceries[key]:
        groceries[key] = re.sub('_copy\d', '', groceries[key])
    if "copy" in groceries2[key]:
        groceries2[key] = re.sub('_copy\d', '', groceries2[key])
assert groceries == groceries2

service.reduction(request)
