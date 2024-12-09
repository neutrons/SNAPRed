from datetime import datetime
from functools import partial
import math
import numpy as np
from pathlib import Path
import re
import sys

from mantid.simpleapi import mtd

import snapred
SNAPRed_module_root = Path(snapred.__file__).parent.parent

from snapred.backend.dao.request.FarmFreshIngredients import FarmFreshIngredients
from snapred.backend.dao.request.ReductionRequest import ReductionRequest
from snapred.backend.data.DataFactoryService import DataFactoryService
from snapred.backend.service.SousChef import SousChef
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceNameGenerator as wng
from snapred.meta.Config import Config

# -----------------------------
# Test helper utility routines:
sys.path.insert(0, str(Path(SNAPRed_module_root).parent / 'tests'))
from util.IPTS_override import IPTS_override
# from util.helpers import timestampFromString

def timestampFromString(timestamp_str) -> float:
    # Recover a float timestamp from a non-isoformat timestamp string
    regx = re.compile(r"([0-9]{4})-([0-9]{2})-([0-9]{2})T([0-9]{2})([0-9]{2})([0-9]{2})")
    Y, M, D, H, m, s = tuple([int(s) for s in regx.match(timestamp_str).group(1, 2, 3, 4, 5, 6)])
    return datetime(Y, M, D, H, m, s).timestamp()

###########################################################
# If necessary, override the IPTS search directories:    ##
#   remember to set your "IPTS.root" in "application.yml!##
###########################################################
with IPTS_override(): # defaults to `Config["IPTS.root"]`

    #######################################################################################################################   
    # Step 1: Generate a set of reduction data.  Take a look under the output folder and see what its timestamp string is.#
    #######################################################################################################################

    runNumber = "46680"
    useLiteMode = True
    timestamp_str = "2024-11-15T133125" # Unfortunately, not in iso format
    timestamp = timestampFromString(timestamp_str)

    ###################################################################################
    # Step 2: Reload the reduction record, and all of the reduction output workspaces.#
    ###################################################################################
    dataService = DataFactoryService()
    sousChef = SousChef()

    reductionRecord = dataService.getReductionData(runNumber, useLiteMode, timestamp)

    #########################################################################################################
    # Step 3: Load the required grouping workspaces, and compute their _unmasked_ pixel-grouping parameters.#
    # (Note here that the `ReductionRecord` itself retains only the _masked_ PGP.)                          #
    #########################################################################################################

    #  ... this duplicates the setup part of the reduction process ...
    groupingMap = dataService.getGroupingMap(runNumber).getMap(useLiteMode)
    request = ReductionRequest(
        runNumber=runNumber,
        useLiteMode=useLiteMode,
        timestamp=timestamp,
        focusGroups = list(groupingMap.values()),
        keepUnfocused=False,
        convertUnitsTo="TOF"
    )
    farmFresh = FarmFreshIngredients(
        runNumber=request.runNumber,
        useLiteMode=request.useLiteMode,
        timestamp=request.timestamp,
        focusGroups=request.focusGroups,
        keepUnfocused=request.keepUnfocused,
        convertUnitsTo=request.convertUnitsTo,
        versions=request.versions,
    )
    ingredients = sousChef.prepReductionIngredients(farmFresh)
    #  ... now the required PGP are available in `ingredients.unmaskedPixelGroups: List[PixelGroup]` ...
    unmaskedPixelGroups = {pg.focusGroup.name: pg for pg in ingredients.unmaskedPixelGroups}

    ##################################################################################################################
    # Step 4: For the output workspace corresponding to each grouping, verify that the effective instrument consists #
    #   of one pixel per group-id, with its location matching the _unmasked_ PGP for that grouping and group-id.     #
    ##################################################################################################################

    # For each grouping, verify that the output workspace's effective instrument has been set up as expected.
    for grouping in unmaskedPixelGroups:
        # We need to rebuild the workspace name, because the `WorkspaceName` of the loaded `ReductionRecord` will only retain its string component.
        reducedOutputWs = wng.reductionOutput().runNumber(runNumber).group(grouping).timestamp(timestamp).build()
        assert reducedOutputWs in reductionRecord.workspaceNames
        assert mtd.doesExist(reducedOutputWs)

        outputWs = mtd[reducedOutputWs]

        effectiveInstrument = outputWs.getInstrument()

        # verify the new instrument name
        assert effectiveInstrument.getName() == f"SNAP_{grouping}"

        # there should be one pixel per output spectrum
        assert effectiveInstrument.getNumberDetectors(True) == outputWs.getNumberHistograms()

        detectorInfo = outputWs.detectorInfo()
        pixelGroup = unmaskedPixelGroups[grouping]

        isclose = partial(math.isclose, rel_tol=10.0 * np.finfo(float).eps, abs_tol=10.0 * np.finfo(float).eps)
        for n, gid in enumerate(pixelGroup.groupIDs):
            # Spectra are in the same index order as the group IDs:
            assert isclose(pixelGroup.L2[n], detectorInfo.l2(n))
            assert isclose(pixelGroup.twoTheta[n], detectorInfo.twoTheta(n))
            assert isclose(pixelGroup.azimuth[n], detectorInfo.azimuthal(n))

    print("*************************************************************")
    print("*** Test of effective instrument substitution successful! ***")
    print("*************************************************************")
        
