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

    def fetchGroceries(self, groceryList: Dict[str, Any]):
        """
        Checkout the workspace names needed for this recipe.
        It is necessary to provide the followign keys:
         - "InputWorkspace": the raw vanadium data
         - "BackgroundWorkspace": the background raw vanadium data
         - "Ingredients": requirements for sub algo calls
        """

        self.rawInput = groceryList["InputWorkspace"]
        self.rawBackgroundInput = groceryList["BackgroundWorkspace"]
        self.groupingWS = groceryList["GroupingWorkspace"]
        self.outputWS = groceryList.get("OutputWorkspace", "")
        self.smoothWS = groceryList.get("SmoothedOutput", "")

    def chopIngredients(self, ingredients: Ingredients):
        self.runNumber = ingredients.reductionIngredients.runConfig.runNumber
        self.backgroundRunNumber = ingredients.backgroundReductionIngredients.runConfig.runNumber

    def executeRecipe(self, ingredients: Ingredients, groceryList: Dict[str, Any]) -> Dict[str, Any]:
        self.chopIngredients(ingredients)
        self.fetchGroceries(groceryList)

        logger.info(
            f"Executing normalization calibration for runId: {self.runNumber} and background runId: {self.backgroundRunNumber}"  # noqa: E501
        )

        data: Dict[str, Any] = {"result": False}

        calibNormAlgo = CalibrationNormalizationAlgo()
        calibNormAlgo.initialize()
        calibNormAlgo.setProperty("InputWorkspace", self.rawInput)
        calibNormAlgo.setProperty("BackgroundWorkspace", self.rawBackgroundInput)
        calibNormAlgo.setProperty("GroupingWorkspace", self.groupingWS)
        calibNormAlgo.setProperty("OutputWorkspace", self.outputWS)
        calibNormAlgo.setProperty("SmoothedOutput", self.smoothWS)
        calibNormAlgo.setProperty("Ingredients", ingredients.json())

        try:
            calibNormAlgo.execute()
            data["FocusWorkspace"] = calibNormAlgo.getProperty("OutputWorkspace").value
            data["SmoothWorkspace"] = calibNormAlgo.getProperty("SmoothedOutput").value
        except RuntimeError as e:
            errorString = str(e)
            logger.error(errorString)
            raise Exception(errorString.split("\n")[0])

        logger.info("Finished executing normalization calibration")
        data["result"] = True
        return data
