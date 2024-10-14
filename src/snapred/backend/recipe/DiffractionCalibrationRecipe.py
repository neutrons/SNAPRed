from typing import Any, Dict, List

from snapred.backend.dao.ingredients import DiffractionCalibrationIngredients as Ingredients
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.algorithm.GroupDiffractionCalibration import GroupDiffractionCalibration
from snapred.backend.recipe.algorithm.PixelDiffractionCalibration import PixelDiffCalRecipe
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
        self.skipPixelCalibration = ingredients.skipPixelCalibration
        self.removeBackground = ingredients.removeBackground

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
        self.diagnosticWS = groceries["diagnosticWorkspace"]
        self.outputDSPWS = groceries["outputWorkspace"]
        self.calTable = groceries.get("calibrationTable", "")
        self.maskWS = groceries.get("maskWorkspace", "")

    def executeRecipe(self, ingredients: Ingredients, groceries: Dict[str, str]) -> Dict[str, Any]:
        self.chopIngredients(ingredients)
        self.unbagGroceries(groceries)

        logger.info(f"Executing diffraction calibration for runId: {self.runNumber}")
        data: Dict[str, Any] = {"result": False}
        medianOffsets: List[float] = []

        if self.skipPixelCalibration is not True:
            logger.info("Calibrating by cross-correlation and adjusting offsets...")
            pixelRes = PixelDiffCalRecipe().cook(ingredients, groceries)
            if pixelRes.result:
                medianOffsets = pixelRes.medianOffsets
                self.calTable = pixelRes.calibrationTable
            else:
                raise RuntimeError("Pixel Calibration failed")

        logger.info("Beginning group-by-group fitting calibration")
        groupedAlgo = GroupDiffractionCalibration()
        groupedAlgo.initialize()
        groupedAlgo.setProperty("InputWorkspace", self.rawInput)
        groupedAlgo.setProperty("OutputWorkspace", self.outputDSPWS)
        groupedAlgo.setProperty("DiagnosticWorkspace", self.diagnosticWS)
        groupedAlgo.setProperty("GroupingWorkspace", self.groupingWS)
        groupedAlgo.setProperty("Ingredients", ingredients.json())
        groupedAlgo.setProperty("PreviousCalibrationTable", self.calTable)
        groupedAlgo.setProperty("MaskWorkspace", self.maskWS)
        groupedAlgo.setProperty("FinalCalibrationTable", self.calTable)
        groupedAlgo.execute()
        data["calibrationTable"] = groupedAlgo.getPropertyValue("FinalCalibrationTable")
        data["diagnosticWorkspace"] = groupedAlgo.getPropertyValue("DiagnosticWorkspace")
        data["outputWorkspace"] = groupedAlgo.getPropertyValue("OutputWorkspace")
        data["maskWorkspace"] = groupedAlgo.getPropertyValue("MaskWorkspace")

        logger.info(f"Finished executing diffraction calibration for runId: {self.runNumber}")
        data["steps"] = medianOffsets
        data["result"] = True
        return data
