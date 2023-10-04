import json
from typing import Any, Dict, List

from mantid.api import AlgorithmManager

from snapred.backend.dao.ingredients import DiffractionCalibrationIngredients as Ingredients
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.algorithm.CalculateOffsetDIFC import (
    name as CalculateOffsetDIFC,
)
from snapred.backend.recipe.algorithm.GroupByGroupCalibration import (
    name as GroupByGroupCalibration,
)
from snapred.meta.decorators.Singleton import Singleton

logger = snapredLogger.getLogger(__name__)


@Singleton
class DiffractionCalibrationRecipe:
    offsetDIFCAlgorithmName: str = CalculateOffsetDIFC
    pdcalibrateAlgorithmName: str = GroupByGroupCalibration

    def __init__(self):
        pass

    def chopIngredients(self, ingredients: Ingredients):
        self.runNumber = ingredients.runConfig.runNumber
        self.threshold = ingredients.convergenceThreshold
        pass

    def executeRecipe(self, ingredients: Ingredients) -> Dict[str, Any]:
        self.chopIngredients(ingredients)

        logger.info(f"Executing diffraction calibration for runId: {self.runNumber}")
        data: Dict[str, Any] = {"result": False}
        dataSteps: List[Dict[str, Any]] = []
        medianOffsets: List[float] = []

        logger.info("Calibrating by cross-correlation and adjusting offsets...")
        offsetAlgo = AlgorithmManager.create(self.offsetDIFCAlgorithmName)
        offsetAlgo.setProperty("Ingredients", ingredients.json())
        try:
            offsetAlgo.execute()
            dataSteps.append(json.loads(offsetAlgo.getProperty("data").value))
            medianOffsets.append(dataSteps[-1]["medianOffset"])
        except RuntimeError as e:
            errorString = str(e)
            raise Exception(errorString.split("\n")[0])

        counter = 0
        while abs(medianOffsets[-1]) > self.threshold:
            counter = counter + 1
            logger.info(f"... converging to answer; step {counter}, {medianOffsets[-1]} > {self.threshold}")
            try:
                offsetAlgo.execute()
                dataSteps.append(json.loads(offsetAlgo.getProperty("data").value))
                medianOffsets.append(dataSteps[-1]["medianOffset"])
            except RuntimeError as e:
                errorString = str(e)
                raise Exception(errorString.split("\n")[0])
        data["steps"] = dataSteps
        logger.info(f"Initial calibration converged.  Offsets: {medianOffsets}")

        logger.info("Beginning group-by-group fitting calibration")
        calibrateAlgo = AlgorithmManager.create(self.pdcalibrateAlgorithmName)
        calibrateAlgo.setProperty("Ingredients", ingredients.json())
        calibrateAlgo.setProperty("InputWorkspace", offsetAlgo.getProperty("OutputWorkspace").value)
        calibrateAlgo.setProperty("PreviousCalibrationTable", offsetAlgo.getProperty("CalibrationTable").value)
        try:
            calibrateAlgo.execute()
            data["calibrationTable"] = calibrateAlgo.getProperty("FinalCalibrationTable").value
            data["outputWorkspace"] = calibrateAlgo.getProperty("OutputWorkspace").value
        except RuntimeError as e:
            errorString = str(e)
            raise Exception(errorString.split("\n")[0])

        logger.info(f"Finished executing diffraction calibration for runId: {self.runNumber}")
        data["result"] = True
        return data
