import json
from typing import Any, Dict, List

from snapred.backend.dao.ingredients import DiffractionCalibrationIngredients as Ingredients
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.algorithm.GroupDiffractionCalibration import GroupDiffractionCalibration
from snapred.backend.recipe.algorithm.PixelDiffractionCalibration import PixelDiffractionCalibration
from snapred.backend.recipe.algorithm.WashDishes import WashDishes
from snapred.meta.decorators.Singleton import Singleton

logger = snapredLogger.getLogger(__name__)


@Singleton
class DiffractionCalibrationRecipe:
    def __init__(self):
        pass

    def chopIngredients(self, ingredients: Ingredients):
        """
        Chops off the needed elements of the diffraction calibration ingredients.
        """
        self.runNumber = ingredients.runConfig.runNumber
        self.threshold = ingredients.convergenceThreshold

    def fetchGroceries(self, groceryList: Dict[str, Any]):
        """
        Checkout the workspace names needed for this recipe.
        It is necessary to provide the followign keys:
         - "inputWorkspace": the raw neutron data
         - "groupingWorkspace": a grouping workspace for focusing the data
        It is optional to provide the following keys:
         - "outputWorkspace": a name for the final output workspace; otherwise default is used
         - "calibrationTable": a name for the fully caliibrated DIFC table; otherwise a default is used
        """
        self.rawInput = groceryList["inputWorkspace"]
        self.groupingWS = groceryList["groupingWorkspace"]
        self.outputWS = groceryList.get("outputWorkspace", "")
        self.calTable = groceryList.get("calibrationTable", "")

    def executeRecipe(self, ingredients: Ingredients, groceryList: Dict[str, Any]) -> Dict[str, Any]:
        self.chopIngredients(ingredients)
        self.fetchGroceries(groceryList)

        logger.info(f"Executing diffraction calibration for runId: {self.runNumber}")
        data: Dict[str, Any] = {"result": False}
        dataSteps: List[Dict[str, Any]] = []
        medianOffsets: List[float] = []

        logger.info("Calibrating by cross-correlation and adjusting offsets...")
        pixelAlgo = PixelDiffractionCalibration()
        pixelAlgo.initialize()
        pixelAlgo.setProperty("InputWorkspace", self.rawInput)
        pixelAlgo.setProperty("GroupingWorkspace", self.groupingWS)
        pixelAlgo.setProperty("Ingredients", ingredients.json())
        tmpPixelAlgoOut = "_tmp_pixel_out"
        pixelAlgo.setProperty("OutputWorkspace", tmpPixelAlgoOut)
        pixelAlgo.setProperty("CalibrationTable", self.calTable)
        try:
            pixelAlgo.execute()
            dataSteps.append(json.loads(pixelAlgo.getProperty("data").value))
            medianOffsets.append(dataSteps[-1]["medianOffset"])
        except RuntimeError as e:
            errorString = str(e)
            raise Exception(errorString.split("\n")[0])
        counter = 0
        while abs(medianOffsets[-1]) > self.threshold:
            counter = counter + 1
            logger.info(f"... converging to answer; step {counter}, {medianOffsets[-1]} > {self.threshold}")
            try:
                pixelAlgo.execute()
                dataSteps.append(json.loads(pixelAlgo.getProperty("data").value))
                medianOffsets.append(dataSteps[-1]["medianOffset"])
            except RuntimeError as e:
                errorString = str(e)
                raise Exception(errorString.split("\n")[0])
        data["steps"] = dataSteps
        logger.info(f"Initial calibration converged.  Offsets: {medianOffsets}")

        logger.info("Beginning group-by-group fitting calibration")
        groupedAlgo = GroupDiffractionCalibration()
        groupedAlgo.initialize()
        groupedAlgo.setProperty("Ingredients", ingredients.json())
        print(pixelAlgo.getProperty("OutputWorkspace").value)
        groupedAlgo.setProperty("InputWorkspace", tmpPixelAlgoOut)
        groupedAlgo.setProperty("OutputWorkspace", self.outputWS)
        groupedAlgo.setProperty("GroupingWorkspace", self.groupingWS)
        groupedAlgo.setProperty("PreviousCalibrationTable", self.calTable)
        groupedAlgo.setProperty("FinalCalibrationTable", self.calTable)
        try:
            groupedAlgo.execute()
            data["calibrationTable"] = groupedAlgo.getProperty("FinalCalibrationTable").value
            data["outputWorkspace"] = groupedAlgo.getProperty("OutputWorkspace").value
        except RuntimeError as e:
            errorString = str(e)
            raise Exception(errorString.split("\n")[0])

        wd = WashDishes()
        wd.initialize()
        wd.setProperty("Workspace", tmpPixelAlgoOut)
        wd.execute()

        logger.info(f"Finished executing diffraction calibration for runId: {self.runNumber}")
        data["result"] = True
        return data
