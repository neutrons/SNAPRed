import json
from typing import Any, Dict, List

from snapred.backend.dao.ingredients import DiffractionCalibrationIngredients as Ingredients
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.algorithm.GroupDiffractionCalibration import GroupDiffractionCalibration
from snapred.backend.recipe.algorithm.PixelDiffractionCalibration import PixelDiffractionCalibration
from snapred.meta.Config import Config
from snapred.meta.decorators.Singleton import Singleton

logger = snapredLogger.getLogger(__name__)


@Singleton
class DiffractionCalibrationRecipe:
    """

    The DiffractionCalibrationRecipe class orchestrates a comprehensive diffraction calibration process
    tailored for scientific data. Beginning with ingredient preparation through chopIngredients, it processes
    DiffractionCalibrationIngredients to extract crucial calibration settings.
    It then manages workspace setup via unbagGroceries, organizing necessary workspaces for calibration tasks.
    The core of the class lies in executeRecipe, where it combines these preparations to perform calibration using
    pixel and group-based algorithms, adjusting offsets and refining calibration through iterative steps until
    convergence criteria are met or maximum iterations are reached.

    """

    def __init__(self):
        pass

    def chopIngredients(self, ingredients: Ingredients):
        """
        Chops off the needed elements of the diffraction calibration ingredients.
        """
        self.runNumber = ingredients.runConfig.runNumber
        self.threshold = ingredients.convergenceThreshold
        self.maxIterations = Config["calibration.diffraction.maximumIterations"]

    def unbagGroceries(self, groceries: Dict[str, Any]):
        """
        Checkout the workspace names needed for this recipe.
        It is necessary to provide the following keys:
        - "inputWorkspace": the raw neutron data
        - "groupingWorkspace": a grouping workspace for focusing the data
        - "outputWorkspace": a name for the final output workspace
        - "calibrationTable": a name for the final calibrated DIFC table
        - "maskWorkspace": a name for the final mask workspace
        """

        self.rawInput = groceries["inputWorkspace"]
        self.groupingWS = groceries["groupingWorkspace"]
        self.outputTOFWS = groceries["outputTOFWorkspace"]
        self.outputDSPWS = groceries["outputDSPWorkspace"]
        self.calTable = groceries.get("calibrationTable", "")
        self.maskWS = groceries.get("maskWorkspace", "")

    def executeRecipe(self, ingredients: Ingredients, groceries: Dict[str, str]) -> Dict[str, Any]:
        self.chopIngredients(ingredients)
        self.unbagGroceries(groceries)

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
        pixelAlgo.setProperty("CalibrationTable", self.calTable)
        pixelAlgo.setProperty("MaskWorkspace", self.maskWS)
        try:
            pixelAlgo.execute()
            dataSteps.append(json.loads(pixelAlgo.getPropertyValue("data")))
            medianOffsets.append(dataSteps[-1]["medianOffset"])
        except RuntimeError as e:
            errorString = str(e)
            raise RuntimeError(errorString) from e
        counter = 1
        while abs(medianOffsets[-1]) > self.threshold and counter < self.maxIterations:
            counter = counter + 1
            logger.info(f"... converging to answer; step {counter}, {medianOffsets[-1]} > {self.threshold}")
            try:
                pixelAlgo.execute()
                newDataStep = json.loads(pixelAlgo.getPropertyValue("data"))
            except RuntimeError as e:
                errorString = str(e)
                raise RuntimeError(errorString) from e
            # ensure monotonic decrease in the median offset
            if newDataStep["medianOffset"] < dataSteps[-1]["medianOffset"]:
                dataSteps.append(newDataStep)
                medianOffsets.append(newDataStep["medianOffset"])
            else:
                logger.warning("Offsets failed to converge monotonically")
                break
        if counter >= self.maxIterations:
            logger.warning("Offset convergence reached max iterations without convergning")
        data["steps"] = dataSteps
        logger.info(f"Initial calibration converged.  Offsets: {medianOffsets}")

        logger.info("Beginning group-by-group fitting calibration")
        groupedAlgo = GroupDiffractionCalibration()
        groupedAlgo.initialize()
        groupedAlgo.setProperty("InputWorkspace", self.rawInput)
        groupedAlgo.setProperty("OutputWorkspaceTOF", self.outputTOFWS)
        groupedAlgo.setProperty("OutputWorkspacedSpacing", self.outputDSPWS)
        groupedAlgo.setProperty("GroupingWorkspace", self.groupingWS)
        groupedAlgo.setProperty("Ingredients", ingredients.json())
        groupedAlgo.setProperty("PreviousCalibrationTable", self.calTable)
        groupedAlgo.setProperty("MaskWorkspace", self.maskWS)
        groupedAlgo.setProperty("FinalCalibrationTable", self.calTable)
        try:
            groupedAlgo.execute()
            data["calibrationTable"] = groupedAlgo.getPropertyValue("FinalCalibrationTable")
            data["outputTOFWorkspace"] = groupedAlgo.getPropertyValue("OutputWorkspaceTOF")
            data["outputDSPWorkspace"] = groupedAlgo.getPropertyValue("OutputWorkspacedSpacing")
            data["maskWorkspace"] = groupedAlgo.getPropertyValue("MaskWorkspace")
        except RuntimeError as e:
            errorString = str(e)
            raise RuntimeError(errorString) from e

        logger.info(f"Finished executing diffraction calibration for runId: {self.runNumber}")
        data["result"] = True
        return data
