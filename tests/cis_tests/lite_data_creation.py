# Use this script to test LiteDataCreationAlgo.py
from mantid.simpleapi import *

from snapred.backend.recipe.algorithm.LiteDataCreationAlgo import LiteDataCreationAlgo
from snapred.backend.recipe.FetchGroceriesRecipe import FetchGroceriesRecipe as FetchRx
from snapred.backend.dao.ingredients.GroceryListItem import GroceryListItem
from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.data.DataFactoryService import DataFactoryService

from snapred.meta.Config import Config

#User input ###########################
runNumber = "47278"
runNumber = "58882"
#######################################

dataFactoryService = DataFactoryService()

print(Config["nexus.native.prefix"])
run = RunConfig(
    runNumber=runNumber,
    IPTS=GetIPTS(RunNumber=runNumber,Instrument='SNAP'), 
    useLiteMode=False,
)

fetchRx = FetchRx()
fetchRx.fetchDirtyNexusData(run)

LDCA = LiteDataCreationAlgo()
LDCA.initialize()
LDCA.setProperty("InputWorkspace", f"tof_all_58882")
LDCA.setProperty("AutoDeleteNonLiteWS", True)
LDCA.setProperty("OutputWorkspace", f"{runNumber}_lite")
LDCA.execute()