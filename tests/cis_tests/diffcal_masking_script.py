# Use this script to test Diffraction Calibration

from typing import List
from pathlib import Path
import json
from pydantic import parse_raw_as
import matplotlib.pyplot as plt
import numpy as np

from mantid.simpleapi import *

import snapred
SNAPRed_module_root = Path(snapred.__file__).parent.parent


from snapred.backend.recipe.algorithm.PixelDiffractionCalibration import PixelDiffractionCalibration as PixelAlgo
from snapred.backend.recipe.algorithm.GroupDiffractionCalibration import GroupDiffractionCalibration as GroupAlgo
from snapred.backend.recipe.DiffractionCalibrationRecipe import DiffractionCalibrationRecipe as Recipe
from snapred.backend.dao.ingredients import DiffractionCalibrationIngredients
from snapred.backend.service.CalibrationService import CalibrationService
from snapred.backend.dao.request.DiffractionCalibrationRequest import DiffractionCalibrationRequest
## for creating ingredients
from snapred.backend.dao.request.FarmFreshIngredients import FarmFreshIngredients
from snapred.backend.service.SousChef import SousChef
## for loading data
from snapred.backend.dao.ingredients.GroceryListItem import GroceryListItem
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

import sys
sys.path.insert(0, str(Path(SNAPRed_module_root).parent / 'tests'))
from util.helpers import (
    createCompatibleMask,
    setSpectraToZero,
    maskSpectra,
    setGroupSpectraToZero,
    maskGroups
)
SNAPInstrumentFilePath = str(Path(mantid.__file__).parent / 'instrument' / 'SNAP_Definition.xml')
SNAPLiteInstrumentFilePath = str(Path(SNAPRed_module_root).parent / 'tests' / 'resources' / 'inputs' / 'pixel_grouping' / 'SNAPLite_Definition.xml')

#User input ###########################
runNumber = "58882"
groupingScheme = 'Column'
cifPath = "/SNS/SNAP/shared/Calibration/CalibrantSamples/Silicon_NIST_640d.cif"
calibrantSamplePath = "SNS/SNAP/shared/Calibration/CalibrationSamples/Silicon_NIST_640D_001.json"
peakThreshold = 0.05
offsetConvergenceLimit = 0.1
isLite = True
instrumentFilePath = SNAPLiteInstrumentFilePath if isLite else SNAPInstrumentFilePath
Config._config["cis_mode"] = False
#######################################

### PREP INGREDIENTS ##################

farmFresh = FarmFreshIngredients(
    runNumber=runNumber,
    useLiteMode=isLite,
    focusGroup={"name": groupingScheme, "definition": ""},
    cifPath=cifPath,
    calibrantSamplePath=calibrantSamplePath,
    peakIntensityThresold=peakThreshold,
    convergenceThreshold=offsetConvergenceLimit,
    maxOffset=100.0,
)
ingredients = SousChef().prepDiffractionCalibrationIngredients(farmFresh)


### FETCH GROCERIES ###################

clerk = GroceryListItem.builder()
clerk.neutron(runNumber).useLiteMode(isLite).add()
clerk.fromRun(runNumber).grouping(groupingScheme).useLiteMode(isLite).add()
groceries = GroceryService().fetchGroceryList(clerk.buildList())

### OPTIONAL: CREATE AN INPUT MASK, IF REQUIRED FOR TESTING. ##########
inputWSName = groceries[0]
groupingWSName = groceries[1]
maskWSName = inputWSName + '_mask'
createCompatibleMask(maskWSName, inputWSName, instrumentFilePath)

### Here any specific spectra or isolated detectors can be masked in the input, if required for testing...
# ---
# maskWS = mtd[maskWSName]
# inputWS = mtd[inputWSName]
# groupingWS = mtd[groupingWSName]

# # mask all detectors contributing to spectra 10, 20, and 30:
# spectraToMask = (10, 20, 30)
# maskSpectra(maskWS, inputWS, spectraToMask)

# # mask detectors corresponding to several groups:
# groupsToMask = (3, 7, 11)
# maskGroups(maskWS, groupingWS, groupsToMask)

# # mask detector #145 only:
# maskWS.setMasked(145) 
# ---
### OR, specific spectra or groups can be initialized so that they will fail...

# spectraToFail = (14, 24, 34)
# setSpectraToZero(inputWS, spectraToFail)

# # prepare specific groups to fail...
# groupsToFail = (5, 9, 13)
# setGroupSpectraToZero(inputWS, groupingWS, groupsToFail)
# ---

### RUN PIXEL CALIBRATION ##########

pixelAlgo = PixelAlgo()
pixelAlgo.initialize()
pixelAlgo.setPropertyValue("Ingredients", ingredients.json())
pixelAlgo.setPropertyValue("InputWorkspace",groceries[0])
pixelAlgo.setPropertyValue("GroupingWorkspace", groceries[1])
pixelAlgo.setPropertyValue("MaskWorkspace", maskWSName)
pixelAlgo.execute()

### PAUSE
"""
Stop here and examine the fits:
  * Make sure that any failing pixels are masked.
  * Make sure that any pixels masked at the original input are still masked.
"""
assert False

median = json.loads(pixelAlgo.getPropertyValue("data"))["medianOffset"]
print(median)

count = 0
while median > offsetConvergenceLimit or count < 5:
    pixelAlgo.execute()
    median = json.loads(pixelAlgo.getPropertyValue("data"))["medianOffset"]
    count += 1

### RUN GROUP CALIBRATION

DIFCprev = pixelAlgo.getPropertyValue("CalibrationTable")

outputWS = mtd.unique_name(prefix="out_ws_")
groupAlgo = GroupAlgo()
groupAlgo.initialize()
groupAlgo.setPropertyValue("Ingredients", ingredients.json())
groupAlgo.setPropertyValue("InputWorkspace", groceries[0])
groupAlgo.setPropertyValue("GroupingWorkspace", groceries[1])
groupAlgo.setPropertyValue("MaskWorkspace", maskWSName)
groupAlgo.setPropertyValue("PreviousCalibrationTable", DIFCprev)
groupAlgo.setPropertyValue("OutputWorkspaceDSpacing", outputWS)
groupAlgo.execute()

### PAUSE
"""
Stop here and examine the fits:
  * Make sure the diffraction focused TOF workspace looks as expected.
  * Make sure the offsets workspace has converged, the DIFCpd only fit pixels inside the group,
      and that the fits match with the TOF diffraction focused workspace.
  * Make sure that any failing pixels are masked.
  * Make sure that any pixels masked at the original input are still masked.
"""
assert False

### IMPORTANT: remember to _remove_ any existing mask workspace here!! ######################
try:
    DeleteWorkspace(maskWSName)
except:
    pass

### RUN RECIPE

clerk = GroceryListItem.builder()
clerk.name("inputWorkspace").neutron(runNumber).useLiteMode(isLite).add()
clerk.name("groupingWorkspace").fromRun(runNumber).grouping(groupingScheme).useLiteMode(isLite).add()

groceries = GroceryService().fetchGroceryDict(
    groceryDict=clerk.buildDict(),
    OutputWorkspace="_output_from_diffcal_recipe",
)

### DUPLICATED from previous section: ##########
#### OPTIONAL: CREATE AN INPUT MASK, IF REQUIRED FOR TESTING. ##########
inputWSName = groceries['inputWorkspace']
groupingWSName = groceries['groupingWorkspace']
maskWSName = inputWSName + '_mask'
createCompatibleMask(maskWSName, inputWSName, instrumentFilePath)

### Here any specific spectra or isolated detectors can be masked in the input, if required for testing...
# ---
# maskWS = mtd[maskWSName]
# inputWS = mtd[inputWSName]
# groupingWS = mtd[groupingWSName]

# # mask all detectors contributing to spectra 10, 20, and 30:
# spectraToMask = (10, 20, 30)
# maskSpectra(maskWS, inputWS, spectraToMask)

# # mask detectors corresponding to several groups:
# groupsToMask = (3, 7, 11)
# maskGroups(maskWS, groupingWS, groupsToMask)

# # mask detector #145 only:
# maskWS.setMasked(145) 
# ---
### OR, specific spectra or groups can be initialized so that they will fail...

# spectraToFail = (14, 24, 34)
# setSpectraToZero(inputWS, spectraToFail)

# # prepare specific groups to fail...
# groupsToFail = (5, 9, 13)
# setGroupSpectraToZero(inputWS, groupingWS, groupsToFail)
# ---

rx = Recipe()
groceries['maskWorkspace'] = maskWSName
rx.executeRecipe(ingredients, groceries)

### PAUSE
"""
Stop here and make sure everything still looks good.
"""
assert False

### TODO: There may need to be a *story* about how the MASK interacts with 'DiffractionCalibrationRequest'.
# #   At present, it's functioning as an implicit parameter which will automatically be created,
# #   and the final mask can still be accessed by looking at the _saved_ calibration data.
# #   As yet, there's no way to specify an initial incoming MASK argument.

### CALL CALIBRATION SERVICE
diffcalRequest = DiffractionCalibrationRequest(
    runNumber = runNumber,
    calibrantSamplePath = calibrantSamplePath,
    useLiteMode = isLite,
    focusGroupPath = ingredients.focusGroup.definition,
    convergenceThreshold = offsetConvergenceLimit,
    peakIntensityThreshold = peakThreshold,
)

calibrationService = CalibrationService()
res = calibrationService.diffractionCalibration(diffcalRequest)
print(json.dumps(res,indent=2))
assert False
