from typing import Any, Dict, List

from snapred.backend.dao.ingredients import DiffractionCalibrationIngredients as Ingredients
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.algorithm.GroupDiffractionCalibration import GroupDiffCalRecipe
from snapred.backend.recipe.algorithm.PixelDiffractionCalibration import PixelDiffCalRecipe
from snapred.meta.decorators.Singleton import Singleton

logger = snapredLogger.getLogger(__name__)


"""
NOTE this recipe will no longer serve any purpose following EWM 7388
It is only remaining to ensure continuity of test coverage during
the PR review process.  It can be reliably removed once the new service
endpoints are setup.
"""


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
        self.skipPixelCalibration = ingredients.skipPixelCalibration

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

        groupGroceries = groceries.copy()
        groupGroceries["previousCalibration"] = groceries.get("calibrationTable", "")
        groupGroceries["calibrationTable"] = groceries.get("calibrationTable")
        groupRes = GroupDiffCalRecipe().cook(ingredients, groupGroceries)
        if not groupRes.result:
            raise RuntimeError("Group Calibration failed")

        logger.info(f"Finished executing diffraction calibration for runId: {self.runNumber}")

        data["calibrationTable"] = groupRes.calibrationTable
        data["diagnosticWorkspace"] = groupRes.diagnosticWorkspace
        data["outputWorkspace"] = groupRes.outputWorkspace
        data["maskWorkspace"] = groupRes.maskWorkspace
        logger.info(f"Finished executing diffraction calibration for runId: {self.runNumber}")
        data["steps"] = medianOffsets
        data["result"] = True
        return data
