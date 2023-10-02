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
from snapred.backend.recipe.algorithm.DetectorPeakPredictor import DetectorPeakPredictor
from snapred.backend.data.DataFactoryService import DataFactoryService
from snapred.backend.service.CrystallographicInfoService import CrystallographicInfoService
from snapred.backend.service.CalibrationService import CalibrationService
from snapred.backend.log.logger import snapredLogger
from snapred.meta.redantic import list_to_raw_pretty
snapredLogger._level = 20

#diffraction calibration imports
from snapred.backend.recipe.algorithm.CalculateOffsetDIFC import CalculateOffsetDIFC
from snapred.backend.dao.ingredients import DiffractionCalibrationIngredients
from snapred.backend.dao import RunConfig, DetectorPeak, GroupPeakList
from pydantic import parse_raw_as
from typing import List

#User inputs ###########################
runNumber = '58882' #58409
cifPath = '/SNS/SNAP/shared/Calibration/CalibrantSamples/Silicon_NIST_640d.cif'
groupingFile = '/SNS/SNAP/shared/Calibration/Powder/PixelGroupingDefinitions/SNAPFocGrp_Column.lite.xml'
#######################################


dataFactoryService=DataFactoryService()
calibrationService = CalibrationService()
pixelGroupingParameters = calibrationService.retrievePixelGroupingParams(runNumber)
calibration = dataFactoryService.getCalibrationState(runNumber)
# focusGroups = reductionIngredients.reductionState.stateConfig.focusGroups

instrumentState = calibration.instrumentState
crystalInfoDict = CrystallographicInfoService().ingest(cifPath)
instrumentState.pixelGroupingInstrumentParameters = pixelGroupingParameters[0]

detectorAlgo = DetectorPeakPredictor()
detectorAlgo.initialize()
detectorAlgo.setProperty("InstrumentState", instrumentState.json())
detectorAlgo.setProperty("CrystalInfo", crystalInfoDict["crystalInfo"].json())
detectorAlgo.setProperty("PeakIntensityThreshold", 0.01)
detectorAlgo.execute()

peakList = detectorAlgo.getProperty("DetectorPeaks").value
peakList = parse_raw_as(List[GroupPeakList], peakList)

runConfig = dataFactoryService.getRunConfig(runNumber)

focusGroup=dataFactoryService.getFocusGroups(runNumber)[0] #column

convergenceThreshold = 0.1

calPath = instrumentState.instrumentConfig.calibrationDirectory

diffractionCalibrationIngredients = DiffractionCalibrationIngredients(
        runConfig=runConfig,
        instrumentState=instrumentState,
        focusGroup=focusGroup,
        groupedPeakLists=peakList,
        calPath=calPath,
        threshold=convergenceThreshold)


algo = CalculateOffsetDIFC()
algo.initialize()
algo.setProperty('Ingredients',diffractionCalibrationIngredients.json())
algo.execute()
calTable = algo.getProperty('CalibrationTable')
wsOut = algo.getProperty('OutputWorkspace')
calData = algo.getProperty('data')
