## This is for testing EWM 2357
#   https://ornlrse.clm.ibmcloud.com/ccm/web/projects/Neutron%20Data%20Project%20%28Change%20Management%29#action=com.ibm.team.workitem.viewWorkItem&id=2357
# Should verify that peaks are removed from the spetrum and there is a smooth fit over the removed parts

from mantid.simpleapi import *
import matplotlib.pyplot as plt
import numpy as np
import json
from snapred.backend.dao.ingredients import SmoothDataExcludingPeaksIngredients
from snapred.backend.recipe.algorithm.DetectorPeakPredictor import SmoothDataExcludingPeaks
from snapred.backend.data.DataFactoryService import DataFactoryService
from snapred.backend.service.CrystallographicInfoService import CrystallographicInfoService
from snapred.backend.service.CalibrationService import CalibrationService
from snapred.backend.log.logger import snapredLogger
snapredLogger._level = 20

#diffraction calibration imports
from typing import List

#User inputs ###########################
runNumber = "58882" #58409
cifPath = "/SNS/SNAP/shared/Calibration/CalibrantSamples/Silicon_NIST_640d.cif"
groupingFile = "/SNS/SNAP/shared/Calibration/Powder/PixelGroupingDefinitions/SNAPFocGrp_Column.lite.xml"
#######################################


dataFactoryService=DataFactoryService()
calibrationService = CalibrationService()
pixelGroupingParameters = calibrationService.retrievePixelGroupingParams(runNumber)
calibration = dataFactoryService.getCalibrationState(runNumber)
# focusGroups = reductionIngredients.reductionState.stateConfig.focusGroups

reductionIngredients = dataFactoryService.getReductionIngredients(runNumber)
ipts = reductionIngredients.runConfig.IPTS
rawDataPath = ipts + "shared/lite/SNAP_{}.lite.nxs.h5".format(reductionIngredients.runConfig.runNumber)
instrumentState = calibration.instrumentState
crystalInfoDict = CrystallographicInfoService().ingest(cifPath)
instrumentState.pixelGroupingInstrumentParameters = pixelGroupingParameters[0]
ws = "raw_data"
LoadEventNexus(Filename=rawDataPath, OutputWorkspace=ws)

ingredients = SmoothDataExcludingPeaksIngredients(instrumentState=instrumentState, crystalInfo=crystalInfoDict['crystalInfo'], smoothingParameter=0.05)

algo = SmoothDataExcludingPeaks()
algo.initialize()
algo.setProperty("Ingredients", ingredients.json())
algo.setProperty("InputWorkspace", ws)
algo.execute()
