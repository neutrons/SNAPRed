## This script is to test EWM 1126, EWM 2166, and EWM 2853.
#   https://ornlrse.clm.ibmcloud.com/ccm/web/projects/Neutron%20Data%20Project%20%28Change%20Management%29#action=com.ibm.team.workitem.viewWorkItem&id=1126
#   https://ornlrse.clm.ibmcloud.com/ccm/web/projects/Neutron%20Data%20Project%20%28Change%20Management%29#action=com.ibm.team.workitem.viewWorkItem&id=2166
#   https://ornlrse.clm.ibmcloud.com/ccm/web/projects/Neutron%20Data%20Project%20%28Change%20Management%29#action=com.ibm.team.workitem.viewWorkItem&id=2853

# This tests that: 
#  1. the algorithm will run;
#  2. the algorithm will create a calibration table;
#  3. the algorithm will create (or optionally augment) a mask of failing detectors;
#  3. the algorithm will converge to within a threshold.
# Adjust the convergenceThreshold parameter below, and compare to the "medianOffset" in the calData dictionary

# A routine is provided to create a compatible MaskWorkspace, which may be used to initialize a mask as an optional input parameter.
# In this usage, a-priori masking is allowed: that is, any detector may be masked in the input mask, and it will then be ignored
#   during the diffraction-calibration process.
# However, unfortunately at present, setting such an input mask is not yet conveniently supported by the "DiffractionCalibrationRecipe" itself.

from mantid.simpleapi import *
from mantid.dataobjects import MaskWorkspace

import matplotlib.pyplot as plt
import numpy as np
import json

import sys
sys.path.append("/SNS/users/4rx/SNAPRed")
from snapred.backend.recipe.algorithm.DetectorPeakPredictor import DetectorPeakPredictor
from snapred.backend.recipe.algorithm.PurgeOverlappingPeaksAlgorithm import PurgeOverlappingPeaksAlgorithm
from snapred.backend.data.DataFactoryService import DataFactoryService
from snapred.backend.service.CrystallographicInfoService import CrystallographicInfoService
from snapred.backend.service.CalibrationService import CalibrationService
from snapred.backend.log.logger import snapredLogger
from snapred.meta.redantic import list_to_raw_pretty
snapredLogger._level = 20

#diffraction calibration imports
from snapred.backend.recipe.algorithm.PixelDiffractionCalibration import PixelDiffractionCalibration
from snapred.backend.recipe.algorithm.GroupDiffractionCalibration import GroupDiffractionCalibration
from snapred.backend.recipe.DiffractionCalibrationRecipe import DiffractionCalibrationRecipe
from snapred.backend.dao.ingredients import DiffractionCalibrationIngredients
from snapred.backend.dao import RunConfig, DetectorPeak, GroupPeakList
from pydantic import parse_raw_as
from typing import List

from snapred.meta.Config import Config

from snapred.meta.redantic import list_to_raw_pretty

    
def createCompatibleMask(maskWSName, inputWSName):
    """Create a mask workspace, compatible with the sample data in the input workspace:
      this mask workspace contains one spectrum per detector.
    """
    
    # Warning: this routine assumes that the input workspace is resident in memory!

    inputWS = mtd[inputWSName]
    inst = inputWS.getInstrument()
    # detectors which are monitors are not included in the mask
    mask = WorkspaceFactory.create("SpecialWorkspace2D", NVectors=inst.getNumberDetectors(True),
                                    XLength=1, YLength=1)
    mtd[maskWSName] = mask
    
    LoadInstrument(
        Workspace=maskWSName,
        Filename="/SNS/SNAP/shared/Calibration/Powder/SNAPLite.xml",
        RewriteSpectraMap=True,
    )
    
    # output_workspace needs to be converted to an actual MaskWorkspace instance
    ExtractMask(InputWorkspace=maskWSName, OutputWorkspace=maskWSName)
    assert isinstance(mtd[maskWSName], MaskWorkspace)
    return maskWSName


# User inputs ######################################################################
runNumber = "58882"  # 58409'
cifPath = "/SNS/SNAP/shared/Calibration/CalibrantSamples/Silicon_NIST_640d.cif"
groupingFile = "/SNS/SNAP/shared/Calibration/Powder/PixelGroupingDefinitions/SNAPFocGrp_Column.lite.xml"
peakThreshold = 0.05
offsetConvergenceLimit = 0.1

# SET TO TRUE TO STOP WASHING DISHES
Config._config['cis_mode'] = True
####################################################################################

# CREATE NEEDED INGREDIENTS ########################################################
dataFactoryService=DataFactoryService()
runConfig = dataFactoryService.getRunConfig(runNumber)
focusGroup=dataFactoryService.getFocusGroups(runNumber)[0] #column
calibration = dataFactoryService.getCalibrationState(runNumber)

calibrationService = CalibrationService()
pixelGroupingParameters = calibrationService.retrievePixelGroupingParams(runNumber)

instrumentState = calibration.instrumentState

calPath = instrumentState.instrumentConfig.calibrationDirectory
instrumentState.pixelGroupingInstrumentParameters = pixelGroupingParameters[0]

crystalInfoDict = CrystallographicInfoService().ingest(cifPath)

detectorAlgo = PurgeOverlappingPeaksAlgorithm()
detectorAlgo.initialize()
detectorAlgo.setProperty("InstrumentState", instrumentState.json())
detectorAlgo.setProperty("CrystalInfo", crystalInfoDict["crystalInfo"].json())
detectorAlgo.setProperty("PeakIntensityFractionThreshold", peakThreshold)
detectorAlgo.execute()
peakList = detectorAlgo.getProperty("OutputPeakMap").value
peakList = parse_raw_as(List[GroupPeakList], peakList)
print(list_to_raw_pretty(peakList))

ingredients = DiffractionCalibrationIngredients(
    runConfig=runConfig,
    instrumentState=instrumentState,
    focusGroup=focusGroup,
    groupedPeakLists=peakList,
    calPath=calPath,
    convergenceThreshold=offsetConvergenceLimit,
)

### Optional: create a compatible input mask if required: ##########################

# Load the input workspace to use as an "instrument donor" for the mask workspace:
runNumber = ingredients.runConfig.runNumber
ipts = ingredients.runConfig.IPTS
rawDataPath = ipts + f'shared/lite/SNAP_{runNumber}.lite.nxs.h5'

inputWSName = f'_TOF_{runNumber}_raw'
LoadEventNexus(
    Filename=rawDataPath,
    OutputWorkspace=inputWSName,
    FilterByTofMin=ingredients.instrumentState.particleBounds.tof.minimum,
    FilterByTofMax=ingredients.instrumentState.particleBounds.tof.maximum,
    BlockList="Phase*,Speed*,BL*:Chop:*,chopper*TDC",
)

# Note: this will also be the DEFAULT MASK NAME, in the case a mask is automatically created by the routines below
maskWSName = f"_MASK_{runNumber}"
# createCompatibleMask(maskWSName, inputWSName)

# Here any detectors can be masked in the input, if required for testing...

####################################################################################
# Run the individual steps of the recipe
# Run the pixel offset calibration to converge
# Run the PDCalibration step group-by-group

data = {"result": False}
dataSteps = []
medianOffsets = []

convergenceThreshold = ingredients.convergenceThreshold
offsetAlgo = PixelDiffractionCalibration()
offsetAlgo.initialize()
offsetAlgo.setProperty("Ingredients", ingredients.json())
offsetAlgo.setProperty("MaskWorkspace", maskWSName)
offsetAlgo.execute()

# save a permanent copy of the input data, and the associated mask
rawTOFInputWS = f'z_TOF_{runNumber}_raw'
rawDSPInputWS = f'z_DSP_{runNumber}_raw'
postOffsetAlgoMaskWS = f'z_MASK_{runNumber}'
CloneWorkspace(
    InputWorkspace=offsetAlgo.inputWStof,
    OutputWorkspace=rawTOFInputWS,
)
CloneWorkspace(
    InputWorkspace=offsetAlgo.inputWSdsp,
    OutputWorkspace=rawDSPInputWS,
)
CloneWorkspace(
    InputWorkspace=maskWSName,
    OutputWorkspace=postOffsetAlgoMaskWS,
)

# record the median offset used in convergence
dataSteps.append(json.loads(offsetAlgo.getProperty("data").value))
medianOffsets.append(dataSteps[-1]["medianOffset"])

counter = 0
while abs(medianOffsets[-1]) > convergenceThreshold:
    counter = counter + 1
    offsetAlgo.execute()
    dataSteps.append(json.loads(offsetAlgo.getProperty("data").value))
    medianOffsets.append(dataSteps[-1]["medianOffset"])

data["steps"] = dataSteps
print(data["steps"], counter)

calibAlgo = GroupDiffractionCalibration()
calibAlgo.initialize()
calibAlgo.setProperty("Ingredients", ingredients.json())
calibAlgo.setProperty("InputWorkspace", offsetAlgo.getProperty("OutputWorkspace").value)
calibAlgo.setProperty("MaskWorkspace", offsetAlgo.getProperty("MaskWorkspace").value)
calibAlgo.setProperty("PreviousCalibrationTable", offsetAlgo.getProperty("CalibrationTable").value)
calibAlgo.execute()
data["calibrationTable"] = calibrateAlgo.getProperty("FinalCalibrationTable").value
data["result"] = True
data["outputFileName"] = calibrateAlgo.outputFilename # TODO: make this a property please!

#############################################################################################
### IMPORTANT: remember to _remove_ any existing mask workspace here!! ######################
mtd.remove(maskWSName)

### Optional: prepare a mask workspace (as above) to use as an input parameter: #############
# Unfortunately here,
#   we need to rely on the fact that the 'maskWSName' is the same as the DEFAULT MASK name.
# Even in this default case: if the mask workspace already exists, its values will be used.

# Run the entire recipe #####################################################################
rx = DiffractionCalibrationRecipe()
res = rx.executeRecipe(ingredients)
print(f'Result: {res["result"]}')
print(f'  steps: {res["steps"]}')
print(f'  mask workspace name: {res["maskWorkspace"]}')
print(f'Calibration data saved to: {data["outputFilename"]}')

#############################################################################################


