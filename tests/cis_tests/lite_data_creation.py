# Use this script to test LiteDataCreationAlgo.py
import snapred.backend.recipe.algorithm
from mantid.simpleapi import LiteDataCreationAlgo
from mantid.testing import assert_almost_equal as assert_wksp_almost_equal

from snapred.backend.data.GroceryService import GroceryService
from snapred.backend.dao.ingredients.GroceryListItem import GroceryListItem

from snapred.meta.Config import Config
Config._Config["cis_mode.enabled"] = False

#User input ###########################
runNumber = "46680"
#######################################

clerk = GroceryListItem.builder()
clerk.neutron(runNumber).native().dirty().add()
clerk.grouping("Lite").add()
groceryList = clerk.buildList()
groceries = GroceryService().fetchGroceryList(groceryList)

workspace = groceries[0]
litemap = groceries[1]

LiteDataCreationAlgo(
    InputWorkspace = workspace,
    LiteDataMapWorkspace = litemap,
    AutoDeleteNonLiteWS = True,
    OutputWorkspace = workspace + "_lite",
)

# check it can't be double-reduced
LiteDataCreationAlgo(
    InputWorkspace = workspace + "_lite",
    LiteDataMapWorkspace = litemap,
    AutoDeleteNonLiteWS = True,
    OutputWorkspace = workspace + "_doubleLite"
)

assert_wksp_almost_equal(
    Workspace1 = workspace + "_lite",
    Workspace2 = workspace + "_doubleLite",
)