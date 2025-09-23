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
from snapred.backend.dao.RunMetadata import RunMetadata
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
    
    if runNumber == RunMetadata.INACTIVE_RUN:
        raise RuntimeError("No run is currently active.  Please wait a while and try again.")
    
    if not dataService.hasLiveDataConnection():
        raise RuntimeError("There doesn't seem to be a live-data connection. Please check your 'application.yml' and try again.")
        
    ## Load the input data, and convert to lite mode: ##   
    clerk = GroceryListItem.builder()
    clerk.name("inputData").neutron(runNumber).useLiteMode(isLite).add()
    groceries = GroceryService().fetchGroceryDict(clerk.buildDict())
    logs = mtd[groceries['inputData']].getRun()
    startTime = logs.startTime().to_datetime64()
    endTime = logs.endTime().to_datetime64()
    
    # `getPulseTimeMin()` and `getPulseTimeMax()` return "stripped" results, for some reason,
    #   but the 'proton_charge' time-series log still has the pulse times.
    pulseTimes = mtd[groceries['inputData']].getRun()['proton_charge'].times
    minPulseTime =  pulseTimes[0]   
    maxPulseTime =  pulseTimes[-1]
    
    print("-------------------------")
    print(f"Time interval (from PV logs):\n    [{startTime}, {endTime}]")    
    print(f"    (from proton-charge times):\n    [{minPulseTime}, {maxPulseTime}]")
    print("-------------------------")
