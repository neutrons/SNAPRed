# Use this script to test LiteDataCreationAlgo.py
import snapred.backend.recipe.algorithm
from mantid.simpleapi import LiteDataCreationAlgo
from mantid.testing import assert_almost_equal as assert_wksp_almost_equal

from snapred.backend.data.DataFactoryService import DataFactoryService
from snapred.backend.data.GroceryService import GroceryService
from snapred.backend.dao.ingredients.GroceryListItem import GroceryListItem
from snapred.backend.dao.ingredients.LiteDataCreationIngredients import LiteDataCreationIngredients

from snapred.meta.Config import Config
Config._config["cis_mode.enabled"] = False
Config._config["cis_mode.preserveDiagnosticWorkspaces"] = False

#User input ###########################
runNumber = "46680"
#######################################
instrumentState = DataFactoryService().getDefaultInstrumentState(runNumber)

clerk = GroceryListItem.builder()
clerk.neutron(runNumber).native().dirty().add()
clerk.grouping("Lite").add()
groceryList = clerk.buildList()
groceries = GroceryService().fetchGroceryList(groceryList)

workspace = groceries[0]
litemap = groceries[1]
ingredients = LiteDataCreationIngredients(
    instrumentState=instrumentState
)

LiteDataCreationAlgo(
    InputWorkspace = workspace,
    LiteDataMapWorkspace = litemap,
    OutputWorkspace = workspace + "_lite",
    Ingredients=ingredients
)
