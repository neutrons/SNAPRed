# import mantid algorithms, numpy and matplotlib
from mantid.simpleapi import *
import matplotlib.pyplot as plt
import numpy as np

from pydantic import parse_file_as
import json

from snapred.backend.recipe.algorithm.CalibrationNormalizationAlgo import CalibrationNormalization
from snapred.backend.dao.ingredients.NormalizationCalibrationIngredients import NormalizationCalibrationIngredients
from snapred.backend.dao.ingredients.SmoothDataExcludingPeaksIngredients import SmoothDataExcludingPeaksIngredients
from snapred.backend.dao.ingredients.PixelGroupingIngredients import PixelGroupingIngredients
from snapred.backend.service.CrystallographicInfoService import CrystallographicInfoService
from snapred.backend.data.DataFactoryService import DataFactoryService
from snapred.backend.dao.state import FocusGroup
from snapred.backend.dao.state.CalibrantSample.CalibrantSamples import CalibrantSamples
from snapred.backend.dao.calibration import CalibrationRecord, Calibration
from snapred.backend.recipe.PixelGroupingParametersCalculationRecipe import PixelGroupingParametersCalculationRecipe
from snapred.meta.Config import Config

#User input ###############################################################################################
runNumber = "58810"
backgroundRunNumber = "58813"
samplePath = "/SNS/SNAP/shared/Calibration_dynamic/CalibrantSamples/Silicon_NIST_640D_001.json"
cifPath = "/SNS/SNAP/shared/Calibration/CalibrantSamples/Silicon_NIST_640d.cif"
groupPath = "/SNS/SNAP/shared/Calibration/Powder/PixelGroupingDefinitions/SNAPFocGrp_Column.lite.xml"
calibrationWorkspace = "/SNS/users/dzj/Desktop/58810_calibration_ws.nxs" # need to create this using diffc_test.py with run number 58810
smoothingParam = 0.50
calibrationStatePath = "/SNS/SNAP/shared/Calibration/Powder/04bd2c53f6bf6754/57514/v_1/CalibrationParameters.json" # need to change this!!!!!!
###########################################################################################################

DFS = DataFactoryService()

#PixelGroupingCalulations #################################################################################

def _generateFocusGroupAndInstrumentState(
runNumber,
definition,
nBinsAcrossPeakWidth=Config["calibration.diffraction.nBinsAcrossPeakWidth"],
calibration=None,):
    if calibration is None:
        calibration = DFS.getCalibrationState(runNumber)
    instrumentState = calibration.instrumentState
    pixelGroupingParams = _calculatePixelGroupingParameters(instrumentState, definition)["parameters"]
    instrumentState.pixelGroupingInstrumentParameters = pixelGroupingParams
    return (
        FocusGroup(
            FWHM=[pgp.twoTheta for pgp in pixelGroupingParams],
            name=definition.split("/")[-1],
            definition=definition,
            nHst=len(pixelGroupingParams),
            dMin=[pgp.dResolution.minimum for pgp in pixelGroupingParams],
            dMax=[pgp.dResolution.maximum for pgp in pixelGroupingParams],
            dBin=[pgp.dRelativeResolution / nBinsAcrossPeakWidth for pgp in pixelGroupingParams],
        ),
        instrumentState,
)

def _calculatePixelGroupingParameters(instrumentState, groupingFile: str):
        groupingIngredients = PixelGroupingIngredients(
            instrumentState=instrumentState,
            instrumentDefinitionFile=Config["instrument.lite.definition.file"],
            groupingFile=groupingFile,
        )
        try:
            data = PixelGroupingParametersCalculationRecipe().executeRecipe(groupingIngredients)
        except:
            raise
        return data

#GetCalibrationRecord######################################################################################

def getCalibrationState(calibrationStatePath):
    calibrationState = parse_file_as(Calibration, calibrationStatePath)
    return calibrationState

###########################################################################################################

#GetSampleFilePath#########################################################################################

def getCalibrantSample(samplePath):
    with open(samplePath, 'r') as file:
        sampleJson = json.load(file)
    del sampleJson["material"]["packingFraction"]
    for atom in sampleJson["crystallography"]["atoms"]:
        atom["symbol"] = atom.pop("atom_type")
        atom["coordinates"] = atom.pop("atom_coordinates")
        atom["siteOccupationFactor"] = atom.pop("site_occupation_factor")
    sample = CalibrantSamples.parse_raw(json.dumps(sampleJson))
    return sample
###########################################################################################################

reductionIngredients = DFS.getReductionIngredients(runNumber)
backgroundReductionIngredients = DFS.getReductionIngredients(backgroundRunNumber)
runConfig = DFS.getRunConfig(runNumber)
calibration = getCalibrationState(calibrationStatePath)
calibrantSample = getCalibrantSample(samplePath)
crystalInfo = CrystallographicInfoService().ingest(cifPath)["crystalInfo"]

focusGroup, instrumentState = _generateFocusGroupAndInstrumentState(
runNumber,
groupPath,
)

smoothDataIngredients = SmoothDataExcludingPeaksIngredients(
    smoothingParameter=smoothingParam,
    instrumentState=instrumentState,
    crystalInfo=crystalInfo,
)

calibrationWorkspaceName = Load(calibrationWorkspace)


ingredients = NormalizationCalibrationIngredients(
reductionIngredients=reductionIngredients,
backgroundReductionIngredients=backgroundReductionIngredients,
calibrantSample=calibrantSample,
focusGroup=focusGroup,
instrumentState=instrumentState,
smoothDataIngredients=smoothDataIngredients,
)

runIpts = reductionIngredients.runConfig.IPTS
backIpts = backgroundReductionIngredients.runConfig.IPTS

runVanadiumFilePath = runIpts + "shared/lite/SNAP_{}.lite.nxs.h5".format(
    reductionIngredients.runConfig.runNumber
)
backgroundVanadiumFilePath = backIpts + "shared/lite/SNAP_{}.lite.nxs.h5".format(
    backgroundReductionIngredients.runConfig.runNumber
)

input_WS = LoadEventNexus(runVanadiumFilePath)

background_input_WS = LoadEventNexus(backgroundVanadiumFilePath)

CNA = CalibrationNormalization()
CNA.initialize()
CNA.setProperty("InputWorkspace", input_WS)
CNA.setProperty("BackgroundWorkspace", background_input_WS)
CNA.setProperty("CalibrationWorkspace", calibrationWorkspaceName)
CNA.setProperty("Ingredients", ingredients.json())
CNA.execute()