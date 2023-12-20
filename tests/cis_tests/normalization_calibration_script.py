# import mantid algorithms, numpy and matplotlib
from mantid.simpleapi import *
import matplotlib.pyplot as plt
import numpy as np

from pydantic import parse_file_as
import json

from snapred.backend.recipe.algorithm.CalibrationNormalizationAlgo import CalibrationNormalizationAlgo
from snapred.backend.dao.ingredients.NormalizationCalibrationIngredients import NormalizationCalibrationIngredients
from snapred.backend.dao.ingredients.SmoothDataExcludingPeaksIngredients import SmoothDataExcludingPeaksIngredients

from snapred.backend.dao.ingredients.PixelGroupingIngredients import PixelGroupingIngredients
from snapred.backend.dao.state.PixelGroup import PixelGroup
from snapred.backend.service.CrystallographicInfoService import CrystallographicInfoService
from snapred.backend.data.DataFactoryService import DataFactoryService
from snapred.backend.service.CalibrationService import CalibrationService
from snapred.backend.dao.state.CalibrantSample.CalibrantSamples import CalibrantSamples
from snapred.backend.dao.calibration import CalibrationRecord, Calibration

from snapred.backend.recipe.FetchGroceriesRecipe import FetchGroceriesRecipe as FetchRx
from snapred.backend.dao.ingredients.GroceryListItem import GroceryListItem

from snapred.backend.recipe.PixelGroupingParametersCalculationRecipe import PixelGroupingParametersCalculationRecipe
from snapred.meta.Config import Config

# for loading data
from snapred.backend.recipe.FetchGroceriesRecipe import FetchGroceriesRecipe as FetchRx
from snapred.backend.dao.ingredients.GroceryListItem import GroceryListItem


#User input ###############################################################################################
isLite = True
runNumber = "58810"
backgroundRunNumber = "58813"
samplePath = "/SNS/SNAP/shared/Calibration_dynamic/CalibrantSamples/Silicon_NIST_640D_001.json"
cifPath = "/SNS/SNAP/shared/Calibration/CalibrantSamples/Silicon_NIST_640d.cif"
groupPath = "/SNS/SNAP/shared/Calibration/Powder/PixelGroupingDefinitions/SNAPFocGrp_Column.lite.xml"
calibrationWorkspace = "/SNS/users/dzj/Desktop/58810_calibration_ws.nxs" # need to create this using diffc_test.py with run number 58810
smoothingParam = 0.50
groupingScheme = "Column"
calibrationStatePath = "/SNS/SNAP/shared/Calibration/Powder/04bd2c53f6bf6754/57514/v_1/CalibrationParameters.json" # need to change this!!!!!!
###########################################################################################################
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

DFS = DataFactoryService()
instrumentState = DFS.getCalibrationState(runNumber).instrumentState


### FETCH GROCERIES ##################
groceryList = [
    GroceryListItem.makeLiteNexusItem(runNumber).toggleLiteMode(isLite),
    GroceryListItem.makeLiteNexusItem(backgroundRunNumber).toggleLiteMode(isLite),
    GroceryListItem.makeLiteGroupingItemFrom(groupingScheme, "prev").toggleLiteMode(isLite),
]
groceries = FetchRx().executeRecipe(groceryList)["workspaces"]

pgpIngredients = PixelGroupingIngredients(
    instrumentState = instrumentState,
    instrumentDefinitionFile = Config["instrument.lite.definition.file"],
    groupingWorkspace=groceries[2],
)

pgp = PixelGroupingParametersCalculationRecipe().executeRecipe(pgpIngredients, groceries[2])["parameters"]
instrumentState.pixelGroup = PixelGroup(pixelGroupingParameters=pgp)
reductionIngredients = DFS.getReductionIngredients(runNumber, instrumentState.pixelGroup)

calibrantSample = getCalibrantSample(samplePath)

crystalInfo = CrystallographicInfoService().ingest(cifPath)["crystalInfo"]
smoothDataIngredients = SmoothDataExcludingPeaksIngredients(
    smoothingParameter=smoothingParam,
    instrumentState=instrumentState,
    crystalInfo=crystalInfo,
)

backgroundReductionIngredients = reductionIngredients
backgroundReductionIngredients.runConfig.runNumber = backgroundRunNumber

ingredients = NormalizationCalibrationIngredients(
    reductionIngredients=reductionIngredients,
    backgroundReductionIngredients
    calibrantSample=calibrantSample,
    smoothDataIngredients=smoothDataIngredients,
)



CNA = CalibrationNormalizationAlgo()
CNA.initialize()
CNA.setProperty("InputWorkspace", groceries[0])
CNA.setProperty("BackgroundWorkspace", groceries[1])
CNA.setProperty("GroupingWorkspace", groceries[2])
CNA.setProperty("OutputWorkspace", groceries[0])
CNA.setProperty("Ingredients", ingredients.json())
CNA.execute()