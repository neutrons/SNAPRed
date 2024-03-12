"""
  CIS test script for: `PixelGroupingCalculationAlgorithm`.
"""

from typing import List
from pathlib import Path
import json
from pydantic import parse_raw_as
import matplotlib.pyplot as plt
import numpy as np

from mantid.simpleapi import *

import snapred
SNAPRed_module_root = Path(snapred.__file__).parent.parent

from snapred.backend.recipe.PixelGroupingParametersCalculationRecipe import (
    PixelGroupingParametersCalculationRecipe as pgpRecipe,
)
from snapred.backend.dao.ingredients import PixelGroupingIngredients
from snapred.backend.dao.ingredients.GroceryListItem import GroceryListItem
from snapred.backend.dao.state.PixelGroup import PixelGroup
from snapred.backend.data.DataFactoryService import DataFactoryService
from snapred.backend.data.GroceryService import GroceryService
from snapred.meta.Config import Config

# Test helper utility routines:
# -----------------------------
#
#  createCompatibleMask(maskWSName: str, templateWSName: str, instrumentFileName: str)
#  #   A mask workspace will be created in the ADS, with no values masked: this workspace will have the
#  #     format expected by the 'DiffractionCalibrationRecipe', 'PixelDiffractionCalibration', and
#  #     'GroupDiffractionCalibration' algorithms, and can be used to mask detectors prior to the
#  #     start of a diffraction-calibration sequence.
#  #      * maskWSName: the name of the mask workspace to be created
#  #      * templateWSName: a template workspace to be used for validation
#  #      * instrumentFileName: a file specifying the instrument to be used to create the mask workspace
#  #        (this instrument should match the instrument of the template workspace).
#
#  setSpectraToZero(inputWS: MatrixWorkspace, nss: Sequence[int])
#  # Zero out all spectra in a list of spectra
#  #    * inputWS: a MatrixWorkspace (_not_ a workspace name)
#  #    * nss: a list of spectrum numbers
#
#  maskSpectra(maskWS: MatrixWorkspace, inputWS: MatrixWorkspace, nss: Sequence[int])
#  # Set mask flags (as workspace values, _not_ as the detector masks themselves)
#  #   for all detectors contributing to each spectrum in the list of spectra
#  #    * maskWS: a MaskWorkspace (_not_ a workspace name)
#  #    * nss: a list of spectrum numbers
#
#  setGroupSpectraToZero(inputWS: MatrixWorkspace, groupingWS: GroupingWorkspace, gids: Sequence[int])
#  # Zero out all spectra contributing to each group in the list of groups
#  #    * inputWS: a MatrixWorkspace (_not_ a workspace name)
#  #    * gids: a list of group numbers
#
#  maskGroups(maskWS: MatrixWorkspace, groupingWS: GroupingWorkspace, gids: Sequence[int])
#  # Set mask flags (as workspace values, _not_ as the detector masks themselves)
#  #   for all detectors contributing to each group in the list of groups
#  #    * maskWS: a MaskWorkspace (_not_ a workspace name)
#  #    * gids: a list of group numbers
#
#  maskComponentByName(maskWSName : str, componentName: str):
#  # Set mask values for all (non-monitor) detectors contributing to a component
#  #  -- maskWSName: name of MaskWorkspace to modify
#  # (warning: only mask values will be changed, not detector mask flags)
#  # -- componentName: the name of a component in the workspace's instrument
#  # To enable the masking of multiple components, mask values are only set,
#  # and never cleared.

import sys
sys.path.insert(0, str(Path(SNAPRed_module_root).parent / 'tests'))
from util.helpers import (
    createCompatibleMask,
    setSpectraToZero,
    maskSpectra,
    setGroupSpectraToZero,
    maskGroups,
    maskComponentByName,
)
SNAPLiteInstrumentFilePath = str(Path(SNAPRed_module_root).parent / 'tests' / 'resources' / 'inputs' / 'pixel_grouping' / 'SNAPLite_Definition.xml')

#User input ###########################
runNumber = "58882"
groupingScheme = 'Column'
isLite = True
instrumentFilePath = SNAPLiteInstrumentFilePath
Config._config["cis_mode"] = False
#######################################


### RUN RECIPE
runConfig = RunConfig(
    runNumber=runNumber,
    IPTS=GetIPTS(RunNumber=runNumber,Instrument='SNAP'), 
    useLiteMode=isLite,
)
dataFactoryService = DataFactoryService()
calibration = dataFactoryService.getCalibrationState(runNumber)
instrumentState = calibration.instrumentState

ingredients = PixelGroupingIngredients(
    instrumentState=calibration.instrumentState,
    groupingScheme=groupingScheme,
    nBinsAcrossPeakWidth=Config["calibration.diffraction.nBinsAcrossPeakWidth"],
)

clerk = GroceryListItem.builder()
clerk.name("groupingWorkspace").fromRun(runNumber).grouping(groupingScheme).useLiteMode(isLite).add()

maskWSName = "_pgp_test_mask" 
groceries = GroceryService().fetchGroceryDict(
    groceryDict=clerk.buildDict(),
    maskWorkspace=maskWSName,
)

#### CREATE AN INPUT MASK, AS REQUIRED FOR TESTING. ##########
groupingWSName = groceries['groupingWorkspace']
maskWSName = groceries['maskWSName']

# The grouping workspace is used as the instrument-donor to make the mask.
createCompatibleMask(maskWSName, groupingWSName, instrumentFilePath)

### Here any specific spectra or isolated detectors can be masked in the input, if required for testing...
# ---
maskWS = mtd[maskWSName]
groupingWS = mtd[groupingWSName]

# # mask detectors corresponding to several groups:
# groupsToMask = (3, 7, 11)
# maskGroups(maskWS, groupingWS, groupsToMask)

# # mask detector-id #145 only:
# maskWS.setMasked(145) 
# ---
### OR, all the pixels can be masked in a specific component of the instrument...

# maskComponentByName(maskWS, "West")
# ---

pgp = pgpRecipe()
result = pgp.executeRecipe(ingredients, groceries)
pixelGroupingParametersList = result["parameters"]

### PAUSE
"""
Stop here and make sure everything looks good.
"""
assert False

