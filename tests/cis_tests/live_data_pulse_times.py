# Use this script to test Diffraction Calibration
from typing import List
from pathlib import Path
import json
import pydantic
import matplotlib.pyplot as plt
import numpy as np

from mantid.simpleapi import *
from mantid.kernel import ConfigService

import snapred
SNAPRed_module_root = Path(snapred.__file__).parent.parent

from snapred.backend.data.LocalDataService import LocalDataService
from snapred.backend.dao.ingredients.GroceryListItem import GroceryListItem
from snapred.backend.data.GroceryService import GroceryService
from snapred.meta.Config import Config, datasearch_directories

# Test helper utility routines:
# -----------------------------
import sys
sys.path.insert(0, str(Path(SNAPRed_module_root).parent / 'tests'))
from util.script_as_test import not_a_test, pause
from util.IPTS_override import IPTS_override

############################
# FAKE live-data listener: 
FILEEVENTDATALISTENER_FILENAME_KEY = "fileeventdatalistener.filename"
FILEEVENTDATALISTENER_CHUNKS_KEY = "fileeventdatalistener.chunks"
ConfigService.setString(FILEEVENTDATALISTENER_FILENAME_KEY, Config["liveData.testInput.inputFilename"])
ConfigService.setString(FILEEVENTDATALISTENER_CHUNKS_KEY, str(Config["liveData.testInput.chunks"]))
# end: FAKE live-data listener
#############################

# Always set the facility:
ConfigService.setFacility(Config["liveData.facility.name"])

### OVERRIDE IPTS location (optional), re-initialize STATE ###         
with (
    # Allow input data directories to possibly be at a different location from "/SNS":
    #   defaults to `Config["IPTS.root"]`.
    IPTS_override(),
    ):

    ##################################################################
    ## Get the current live-data run number:                        ##
    ## -- this needs to happen _after_ the override settings above! ##
    ##################################################################
    dataService = LocalDataService()
    metadata = dataService.readLiveMetadata()
    runNumber = metadata.runNumber
    isLite = True
    
    ## Load the input data, and convert to lite mode: ##   
    clerk = GroceryListItem.builder()
    clerk.neutron(runNumber).useLiteMode(isLite).add()
    groceries = GroceryService().fetchGroceryDict(clerk.buildDict())
