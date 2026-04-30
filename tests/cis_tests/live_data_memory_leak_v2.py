# Use this script to test Diffraction Calibration
from typing import List
from pathlib import Path
from datetime import datetime
import json
import logging
import pydantic
import matplotlib.pyplot as plt
import numpy as np
import os
import socket
import time
from urllib.parse import urlparse
import tracemalloc

from mantid.api import Run
from mantid.simpleapi import *
from mantid.kernel import ConfigService

import snapred
SNAPRed_module_root = Path(snapred.__file__).parent.parent

from snapred.backend.dao.RunMetadata import RunMetadata
from snapred.backend.dao.state.DetectorState import DetectorState
from snapred.meta.Config import Config, datasearch_directories
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName

# Test helper utility routines:
# -----------------------------
import sys
sys.path.insert(0, str(Path(SNAPRed_module_root).parent / 'tests'))
from util.script_as_test import not_a_test, pause
from util.IPTS_override import IPTS_override


# Basic configuration to output DEBUG level and above
logging.basicConfig(level=logging.DEBUG)

logger = logging.getLogger("live_data_memory_leak_v2")


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

def hasLiveDataConnection() -> bool:
    """For 'live data' methods: test if there is a listener connection to the instrument."""

    # NOTE: adding `lru_cache` to this method bypasses a possible race condition in
    #   `ConfigService.getFacility(...)`.  (And yes, that should be a `const` method.  :( )

    # In addition to 'analysis.sns.gov', other nodes on the subnet should be OK as well.
    #   So this check should also return True on those nodes.
    # If this method returns True, then the `SNSLiveEventDataListener` should be able to function.

    status = False
    if Config["liveData.enabled"]:
        # Normalize to an actual "URL" and then strip off the protocol (not actually "http") and port:
        #   `liveDataAddress` returns a string similar to "bl3-daq1.sns.gov:31415".

        facility, instrument = Config["liveData.facility.name"], Config["liveData.instrument.name"]
        hostname = urlparse(
            "http://" + ConfigService.getFacility(facility).instrument(instrument).liveDataAddress()
        ).hostname
        status = True
        try:
            socket.gethostbyaddr(hostname)
        except Exception as e:  # noqa: BLE001
            # specifically:
            #   we're expecting a `socket.gaierror`, but any exception will indicate that there's no connection
            logger.debug(f"`hasLiveDataConnection` returns `False`: exception: {e}")
            status = False

    return status

def _liveMetadataFromRun(run: Run) -> RunMetadata:
    """Construct a 'RunMetadata' instance from a 'mantid.api.Run' instance."""

    metadata = None
    runNumber = run.getProperty("run_number").value if run.hasProperty("run_number") else None
    try:
        # WORK-AROUND for this script specifically:
        stateIdSchema = DetectorState.LEGACY_SCHEMA
        """
        stateIdSchema = (
            # A run number of `None` or 0 indicates either an inactive, or an incompletely initialized run:
            #   in that case, the rest of the metadata may still be valid,
            #   so we fallback to using the LEGACY_SCHEMA.
            self.readInstrumentConfig(runNumber).stateIdSchema if bool(runNumber) else DetectorState.LEGACY_SCHEMA
        )
        """
        metadata = RunMetadata.fromRun(run, stateIdSchema, liveData=True)
    except (KeyError, RuntimeError, ValueError) as e:
        raise RuntimeError(f"Unable to extract RunMetadata from Run:\n  {e}") from e
    return metadata

def _readLiveData(ws: WorkspaceName, duration: int, *, accumulationMethod="Replace", preserveEvents=False):
    # Initialize `startTime` to indicate that we want `duration` seconds of data prior to the current time.
    startTime = (
        RunMetadata.FROM_NOW_ISO8601
        if duration == 0
        else (datetime.utcnow() + timedelta(seconds=-duration)).isoformat()
    )

    # TODO: this call is partially duplicated at `FetchGroceriesAlgorithm`.
    #   However, this separate method is required in order to specify a "fast load" for metadata purposes.
    LoadLiveData(
        OutputWorkspace=ws,
        Instrument=Config["liveData.instrument.name"],
        AccumulationMethod=accumulationMethod,
        StartTime=startTime,
        PreserveEvents=preserveEvents,
    )

    return ws

def readLiveMetadata() -> RunMetadata:
    # This method serves both as the direct `RunMetadata` access point for the non-fallback live-data case,
    #   and also for the fallback case.

    ws = mtd.unique_hidden_name()

    # Retrieve the smallest possible data increment, in order to read the logs:
    ws = _readLiveData(ws, duration=0)
    metadata = _liveMetadataFromRun(mtd[ws].getRun())

    DeleteWorkspace(Workspace=ws)
    return metadata

# -----------------------------

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

    N_tests = 4320
    tracemalloc.start()    
    start_perf = time.perf_counter()
    for n_test in range(N_tests):
        if not hasLiveDataConnection():
            raise RuntimeError("There doesn't seem to be a live-data connection. Please check your 'application.yml' and try again.")

        metadata = readLiveMetadata()
        runNumber = metadata.runNumber
                
        end_perf = time.perf_counter()
        elapsed = end_perf - start_perf
        pid = os.getpid()
        rss = get_pid_rss_kb(pid)
        print(f"{elapsed:.4f} seconds: RSS {rss} kB")
        
        time.sleep(5.0)
    
    snapshot = tracemalloc.take_snapshot()
    top_stats = snapshot.statistics('lineno')

    print("-----------------------------------------------------------")
    print("--------------- Memory-allocation traces ------------------")
    print("-----------------------------------------------------------")

    print("[ Top 10 ]")
    for stat in top_stats[:10]:
        print(stat)    

    print("-----------------------------------------------------------")
