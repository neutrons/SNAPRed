## This script is to test EWM 1126 and EWM 2166
#   https://ornlrse.clm.ibmcloud.com/ccm/web/projects/Neutron%20Data%20Project%20%28Change%20Management%29#action=com.ibm.team.workitem.viewWorkItem&id=1126
#   https://ornlrse.clm.ibmcloud.com/ccm/web/projects/Neutron%20Data%20Project%20%28Change%20Management%29#action=com.ibm.team.workitem.viewWorkItem&id=2166
# This tests that 
#  1. the algorithm will run,
#  2. the algorithm will create a calibration table, 
#  #. the algorithm will convergence to within a threshold.
# Adjust the convergenceThreshold parameter below, and compare to the "medianOffset" in the calData dictionary

from mantid.simpleapi import *
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
from snapred.backend.recipe.algorithm.CalculateOffsetDIFC import CalculateOffsetDIFC
from snapred.backend.recipe.algorithm.GroupByGroupCalibration import GroupByGroupCalibration
from snapred.backend.recipe.DiffractionCalibrationRecipe import DiffractionCalibrationRecipe
from snapred.backend.dao.ingredients import DiffractionCalibrationIngredients
from snapred.backend.dao import RunConfig, DetectorPeak, GroupPeakList
from pydantic import parse_raw_as
from typing import List

from snapred.meta.Config import Config

from snapred.meta.redantic import list_to_raw_pretty

# User inputs ######################################################################
runNumber = "58882"  # 58409'
cifPath = "/SNS/SNAP/shared/Calibration/CalibrantSamples/Silicon_NIST_640d.cif"
groupingFile = "/SNS/SNAP/shared/Calibration/Powder/PixelGroupingDefinitions/SNAPFocGrp_Column.lite.xml"
peakThreshold = 0.05
offsetConvergenceLimit = 0.1

# SET TO TRUE TO STOP WASHING DISHES
Config._config['cis_mode'] = False
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
####################################################################################
# Run the individual steps of the recipe
# Run the pixel offset calibration to converge
# Run the PDCalibration step group-by-group

data = {"result": False}
dataSteps = []
medianOffsets = []

runNumber = ingredients.runConfig.runNumber
convergenceThreshold = ingredients.convergenceThreshold
offsetAlgo = CalculateOffsetDIFC()
offsetAlgo.initialize()
offsetAlgo.setProperty("Ingredients", ingredients.json())
offsetAlgo.execute()

# save a permanent copy of the input data
rawTOFInputWS = f'z_TOF_{runNumber}_raw'
rawDSPInputWS = f'z_DSP_{runNumber}_raw'
CloneWorkspace(
    InputWorkspace=offsetAlgo.inputWStof,
    OutputWorkspace=rawTOFInputWS,
)
CloneWorkspace(
    InputWorkspace=offsetAlgo.inputWSdsp,
    OutputWorkspace=rawDSPInputWS,
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

calibAlgo = GroupByGroupCalibration()
calibAlgo.initialize()
calibAlgo.setProperty("Ingredients", ingredients.json())
calibAlgo.setProperty("InputWorkspace", offsetAlgo.getProperty("OutputWorkspace").value)
calibAlgo.setProperty("PreviousCalibrationTable", offsetAlgo.getProperty("CalibrationTable").value)
calibAlgo.execute()
data["calibrationTable"] = calibrateAlgo.getProperty("FinalCalibrationTable").value
data["result"] = True

# Run the entire recipe #####################################################################
rx = DiffractionCalibrationRecipe()
res = rx.executeRecipe(ingredients)
print(res["result"])
print(res["steps"])
#############################################################################################


