# Use this script to test Diffraction Calibration
from typing import List
from pathlib import Path
from datetime import datetime
import json
import pydantic
import matplotlib.pyplot as plt
import numpy as np
import os
import time
# import tracemalloc

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

def get_pid_rss_kb(pid: int):
    """
    Retrieves the Resident Set Size (RSS) in KB for a given PID 
    using the Linux /proc/[pid]/smaps_rollup file.
    """
    path = f"/proc/{pid}/smaps_rollup"
    try:
        with open(path, "r") as f:
            for line in f:
                if line.startswith("Rss:"):
                    # Format is usually: "Rss:                1234 kB"
                    parts = line.split()
                    return int(parts[1])
    except FileNotFoundError:
        return f"Error: {path} not found. (Requires Linux kernel 4.14+)"
    except PermissionError:
        return f"Error: Permission denied accessing {path}."
    except Exception as e:
        return f"Error: {str(e)}"

# -----------------------------

############################
# FAKE live-data listener: 

# FILEEVENTDATALISTENER_FILENAME_KEY = "fileeventdatalistener.filename"
# FILEEVENTDATALISTENER_CHUNKS_KEY = "fileeventdatalistener.chunks"
# ConfigService.setString(FILEEVENTDATALISTENER_FILENAME_KEY, Config["liveData.testInput.inputFilename"])
# ConfigService.setString(FILEEVENTDATALISTENER_CHUNKS_KEY, str(Config["liveData.testInput.chunks"]))

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

    N_tests = 25
    # tracemalloc.start()    
    start_perf = time.perf_counter()
    for n_test in range(N_tests):
        metadata = dataService.readLiveMetadata()
        runNumber = metadata.runNumber
        
        ## if runNumber == RunMetadata.INACTIVE_RUN:
        ##    raise RuntimeError("No run is currently active.  Please wait a while and try again.")

        if not dataService.hasLiveDataConnection():
            raise RuntimeError("There doesn't seem to be a live-data connection. Please check your 'application.yml' and try again.")
        
        end_perf = time.perf_counter()
        elapsed = end_perf - start_perf
        pid = os.getpid()
        rss = get_pid_rss_kb(pid)
        print(f"{elapsed:.4f} seconds: RSS {rss} kB")
        
        time.sleep(30.0)
    
    """
    snapshot = tracemalloc.take_snapshot()
    top_stats = snapshot.statistics('lineno')

    print("-----------------------------------------------------------")
    print("--------------- Memory-allocation traces ------------------")
    print("-----------------------------------------------------------")

    print("[ Top 10 ]")
    for stat in top_stats[:10]:
        print(stat)    

    print("-----------------------------------------------------------")
    """
        
    """    
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
    """
    
