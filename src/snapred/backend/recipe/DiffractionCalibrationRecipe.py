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

    def chopIngredients(self, ingredients: Ingredients, data: Dict[str, Any]):
        self.runNumber = ingredients.runConfig.runNumber
        self.threshold = ingredients.convergenceThreshold
        self.rawInput = data["inputWorkspace"]
        self.groupingWS = data["groupingWorkspace"]
        self.outputWS = data.get("outputWorkspace", "")
        self.calTable = data.get("calibrationTable", "")
        pass

    def executeRecipe(self, ingredients: Ingredients, data: Dict[str, Any]) -> Dict[str, Any]:
        self.chopIngredients(ingredients, data)

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
