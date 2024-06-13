# Use this script to test Diffraction Calibration
from typing import List
from pathlib import Path
import json
import pydantic
import matplotlib.pyplot as plt
import numpy as np

from mantid.simpleapi import *

import snapred
SNAPRed_module_root = Path(snapred.__file__).parent.parent



## for creating ingredients
from snapred.backend.dao.request.FarmFreshIngredients import FarmFreshIngredients
from snapred.backend.service.SousChef import SousChef

## for loading data
from snapred.backend.data.LocalDataService import LocalDataService
from snapred.backend.dao.ingredients.GroceryListItem import GroceryListItem
from snapred.backend.data.GroceryService import GroceryService

## the code to test
from snapred.backend.recipe.algorithm.PixelDiffractionCalibration import PixelDiffractionCalibration as PixelAlgo
from snapred.backend.recipe.algorithm.GroupDiffractionCalibration import GroupDiffractionCalibration as GroupAlgo
from snapred.backend.recipe.DiffractionCalibrationRecipe import DiffractionCalibrationRecipe as Recipe

# for running through service layer
from snapred.backend.service.CalibrationService import CalibrationService
from snapred.backend.dao.request import (
    DiffractionCalibrationRequest,
    CalibrationAssessmentRequest,
)

# for populating requests with default values
from snapred.backend.dao.Limit import Pair
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceType as wngt
from snapred.backend.dao.RunConfig import RunConfig
from snapred.meta.Config import Config, datasearch_directories

# Test helper utility routines:
# -----------------------------
import sys
sys.path.insert(0, str(Path(SNAPRed_module_root).parent / 'tests'))
from util.state_helpers import state_root_override
from util.script_as_test import not_a_test, pause
from util.golden_data import assertMatchToGoldenData
from util.IPTS_override import IPTS_override
from util.state_helpers import state_root_override

@not_a_test
def script(goldenData):
    # When actually called from a test method, "goldenData" is a test fixture,
    #   otherwise it will be ignored.

    #User input ###########################
    runNumber = "58882"
    groupingScheme = "Column"
    cifPath = f"{Config['instrument.home']}/shared/Calibration/CalibrantSamples/Silicon_NIST_640d.cif"
    calibrantSamplePath = f"{Config['instrument.home']}/shared/Calibration/CalibrationSamples/Silicon_NIST_640D_001.json"
    peakThreshold = 0.05
    offsetConvergenceLimit = 0.1
    isLite = True
    Config._config["cis_mode"] = False
    #######################################

    ### OVERRIDE IPTS location (optional), re-initialize STATE ###         
    with (
        # Allow input data directories to possibly be at a different location from "/SNS":
        #   defaults to `Config["IPTS.root"]`.
        IPTS_override(),
        # Initialize a new state root directory:
        # * default behavior is to delete at exit, but this can be changed.
        state_root_override(runNumber, "new_diffcal_script-test-state", isLite, delete_at_exit=True) as stateRootPath
        ):
        pause(f"State root directory: {stateRootPath} (will exist only for the duration of the test)")
        
        ########################################################

        ### PREP INGREDIENTS ################
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

        ### FETCH GROCERIES ##################

        clerk = GroceryListItem.builder()
        clerk.neutron(runNumber).useLiteMode(isLite).add()
        clerk.fromRun(runNumber).grouping(groupingScheme).useLiteMode(isLite).add()
        groceries = GroceryService().fetchGroceryList(clerk.buildList())

        ### RUN PIXEL CALIBRATION ##########
        pixelAlgo = PixelAlgo()
        pixelAlgo.initialize()
        pixelAlgo.setPropertyValue("Ingredients", ingredients.json())
        pixelAlgo.setPropertyValue("InputWorkspace",groceries[0])
        pixelAlgo.setPropertyValue("GroupingWorkspace", groceries[1])
        pixelAlgo.execute()

        pause("End of PIXEL CALIBRATION section")

        median = json.loads(pixelAlgo.getPropertyValue("data"))["medianOffset"]
        print(median)

        count = 0
        while median > offsetConvergenceLimit or count < 5:
            pixelAlgo.execute()
            median = json.loads(pixelAlgo.getPropertyValue("data"))["medianOffset"]
            count += 1

        ### RUN GROUP CALIBRATION

        DIFCprev = pixelAlgo.getPropertyValue("CalibrationTable")

        groupAlgo = GroupAlgo()
        groupAlgo.initialize()
        groupAlgo.setPropertyValue("Ingredients", ingredients.json())
        groupAlgo.setPropertyValue("InputWorkspace", groceries[0])
        groupAlgo.setPropertyValue("GroupingWorkspace", groceries[1])
        groupAlgo.setPropertyValue("PreviousCalibrationTable", DIFCprev)
        groupAlgo.setPropertyValue("OutputWorkspaceDSpacing", "out_ws")
        groupAlgo.execute()

        ### PAUSE
        """
        Stop here and examine the fits.
        Make sure the diffraction focused TOF workspace looks as expected.
        Make sure the offsets workspace has converged, the DIFCpd only fit pixels inside the group,
        and that the fits match with the TOF diffraction focused workspace.
        """
        pause("End of GROUP CALIBRATION section")


        ### RUN RECIPE

        clerk = GroceryListItem.builder()
        clerk.name("inputWorkspace").neutron(runNumber).useLiteMode(isLite).add()
        clerk.name("groupingWorkspace").fromRun(runNumber).grouping(groupingScheme).useLiteMode(isLite).add()

        groceries = GroceryService().fetchGroceryDict(
            groceryDict=clerk.buildDict(),
            outputTOFWorkspace="_output_from_diffcal_recipe",
            outputDSPWorkspace="_output_diffcal_but_in_dspacing",
        )

        rx = Recipe()
        rx.executeRecipe(ingredients, groceries)


        ### PAUSE
        """
        Stop here and make sure everything still looks good.
        """
        pause("End of CALIBRATION RECIPE section")

        ### CALL CALIBRATION SERVICE
        diffcalRequest = DiffractionCalibrationRequest(
            runNumber = runNumber,
            calibrantSamplePath = calibrantSamplePath,
            useLiteMode = isLite,
            focusGroup = {"name": groupingScheme, "definition": ""},
            convergenceThreshold = offsetConvergenceLimit,
            peakIntensityThreshold = peakThreshold,
        )

        calibrationService = CalibrationService()
        calibrationResult = calibrationService.diffractionCalibration(diffcalRequest)
        print(json.dumps(calibrationResult,indent=4))
        pause("End of CALIBRATION SERVICE section")
        
        # To simplify golden-data comparison, only compare the details of the _last_ step:
        #   * To pass for multiple users, reltol is temporarily increased;
        #   * See: defect EWM# 5034; RE: Diffraction-calibration results are different for different users.
        
        calibrationResult["steps"] = [calibrationResult["steps"][-1]]
        assertMatchToGoldenData(calibrationResult, goldenData, reltol=3.0)

        ### CALL CALIBRATION SERVICE -- GENERATE METRICS
        assessmentRequest = CalibrationAssessmentRequest(
            run = RunConfig(runNumber=runNumber),
            useLiteMode = isLite,
            focusGroup = {"name": groupingScheme, "definition": ""},
            calibrantSamplePath = calibrantSamplePath,
            workspaces = {wngt.DIFFCAL_OUTPUT: [calibrationResult["outputDSPWorkspace"]]},
            peakFunction = "Gaussian",
            crystalDMin = Config["constants.CrystallographicInfo.dMin"],
            crystalDMax = Config["constants.CrystallographicInfo.dMax"],
            peakIntensityThreshold = Config["calibration.diffraction.peakIntensityThreshold"],
            nBinsAcrossPeakWidth = Config["calibration.diffraction.nBinsAcrossPeakWidth"],
            fwhmMultipliers = Pair.model_validate(Config["calibration.parameters.default.FWHMMultiplier"])
        )
        assessmentResponse = calibrationService.assessQuality(assessmentRequest)
        print(assessmentResponse.record.focusGroupCalibrationMetrics.json(indent=4))
        pause("End of CALIBRATION METRIC section")
        
        # To pass for multiple users, reltol is temporarily increased;
        # * See: defect EWM# 5034; RE: Diffraction-calibration results are different for different users.
        
        assertMatchToGoldenData(assessmentResponse.record.focusGroupCalibrationMetrics, goldenData, reltol=3.0)

if __name__ == "__main__" or __name__ == "mantidqt.widgets.codeeditor.execution":
    # The input argument of "None" silences the requirement for the "goldenData" fixture
    script(None)
