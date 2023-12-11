import json
from typing import Any, Dict

from snapred.backend.dao.ingredients import NormalizationCalibrationIngredients as Ingredients
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.algorithm.CalibrationNormalizationAlgo import CalibrationNormalizationAlgo
from snapred.backend.recipe.algorithm.WashDishes import WashDishes
from snapred.meta.decorators.Singleton import Singleton

logger = snapredLogger.getLogger(__name__)


@Singleton
class CalibrationNormalizationRecipe:
    def __init__(self):
        pass

    def chopIngredients(self, ingredients: Ingredients):
        self.runNumber = ingredients.reductionIngredients.runConfig.runNumber
        self.backgroundRunNumber = ingredients.backgroundReductionIngredients.runConfig.runNumber

    def unbagGroceries(self, groceryList: Dict[str, Any]):
        """
        Checkout the workspace names needed for this recipe.
        It is necessary to provide the followign keys:
         - "InputWorkspace": the raw vanadium data
         - "BackgroundWorkspace": the background raw vanadium data
         - "Ingredients": requirements for sub algo calls
        """

        self.rawInput = groceryList["inputWorkspace"]
        self.rawBackgroundInput = groceryList["backgroundWorkspace"]
        self.groupingWS = groceryList["groupingWorkspace"]
        self.outputWS = groceryList.get("outputWorkspace", "")
        self.smoothWS = groceryList.get("smoothedOutput", "")

    def executeRecipe(self, ingredients: Ingredients, groceryList: Dict[str, Any]) -> Dict[str, Any]:
        self.chopIngredients(ingredients)
        self.unbagGroceries(groceryList)

        logger.info(
            f"Executing normalization calibration for runId: {self.runNumber} and background runId: {self.backgroundRunNumber}"  # noqa: E501
        )

        data: Dict[str, Any] = {"result": False}

        calibNormAlgo = CalibrationNormalizationAlgo()
        calibNormAlgo.initialize()
        calibNormAlgo.setPropertyValue("InputWorkspace", self.rawInput)
        calibNormAlgo.setPropertyValue("BackgroundWorkspace", self.rawBackgroundInput)
        calibNormAlgo.setPropertyValue("GroupingWorkspace", self.groupingWS)
        calibNormAlgo.setPropertyValue("OutputWorkspace", self.outputWS)
        calibNormAlgo.setPropertyValue("SmoothedOutput", self.smoothWS)
        calibNormAlgo.setPropertyValue("Ingredients", ingredients.json())

        try:
            calibNormAlgo.execute()
            data["outputWorkspace"] = calibNormAlgo.getPropertyValue("OutputWorkspace")
            data["smoothedOutput"] = calibNormAlgo.getPropertyValue("SmoothedOutput")
        except RuntimeError as e:
            errorString = str(e)
            raise Exception(errorString.split("\n")[0])

        logger.info("Finished executing normalization calibration")
        data["result"] = True
        return data
