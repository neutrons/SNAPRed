# Use this script to test LiteDataCreationAlgo.py
from mantid.simpleapi import *

from snapred.backend.recipe.algorithm.LiteDataCreationAlgo import LiteDataCreationAlgo
from snapred.backend.data.GroceryService import GroceryService
from snapred.backend.dao.ingredients.GroceryListItem import GroceryListItem
from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.data.DataFactoryService import DataFactoryService

from snapred.meta.Config import Config
Config._config["cis_mode"] = False

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

LDCA = LiteDataCreationAlgo()
LDCA.initialize()
LDCA.setProperty("InputWorkspace", workspace)
LDCA.setProperty("LiteDataMapWorkspace", litemap)
LDCA.setProperty("AutoDeleteNonLiteWS", True)
LDCA.setProperty("OutputWorkspace", workspace + "_lite")
LDCA.execute()

# check it can't be double-reduced
LDCA.setProperty("InputWorkspace", workspace + "_lite")
LDCA.setProperty("OutputWorkspace", workspace + "_doubleLite")
LDCA.execute()

assert CompareWorkspaces(
    Workspace1 = workspace + "_lite",
    Workspace2 = workspace + "_doubleLite",
)