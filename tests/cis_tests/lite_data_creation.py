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
runNumber = "58882"
#######################################

run = GroceryListItem.builder().neutron(runNumber).native().dirty().buildList()
res = GroceryService().fetchGroceryList(run)

workspace = res[0]

LDCA = LiteDataCreationAlgo()
LDCA.initialize()
LDCA.setProperty("InputWorkspace", workspace)
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