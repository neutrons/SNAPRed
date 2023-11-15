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

    def unbagGroceries(self, groceryList: Dict[str, Any]):
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

    # TODO: move saving to inside the calibration service using TBD saving method
    def restockShelves(self, calibrationWS: str, maskWS: str = ""):
        """
        The final diffraction calibration table needs to be saved to disk,
        This will later be handled in a more robust way with a service.
        For the moment this is being handled by saving at the end of the recipe.
        This in a separate method so it can be easily mocked for testing.
        """
        from mantid.simpleapi import SaveDiffCal

        from snapred.backend.data.LocalDataService import LocalDataService

        lds = LocalDataService()
        calibrationPath = lds._getCalibrationDataPath(self.runNumber)
        filename = calibrationPath + "/difcal.h5"
        SaveDiffCal(
            CalibrationWorkspace=calibrationWS,
            GroupingWorkspace=self.groupingWS,
            MaskWorkspace=maskWS,
            Filename=filename,
        )

    def executeRecipe(self, ingredients: Ingredients, groceryList: Dict[str, Any]) -> Dict[str, Any]:
        self.chopIngredients(ingredients)
        self.unbagGroceries(groceryList)

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
        groupedAlgo.setProperty("InputWorkspace", self.rawInput)
        groupedAlgo.setProperty("OutputWorkspace", self.outputWS)
        groupedAlgo.setProperty("GroupingWorkspace", self.groupingWS)
        groupedAlgo.setProperty("Ingredients", ingredients.json())
        groupedAlgo.setProperty("PreviousCalibrationTable", self.calTable)
        groupedAlgo.setProperty("FinalCalibrationTable", self.calTable)
        try:
            groupedAlgo.execute()
            data["calibrationTable"] = groupedAlgo.getProperty("FinalCalibrationTable").value
            data["outputWorkspace"] = groupedAlgo.getProperty("OutputWorkspace").value
        except RuntimeError as e:
            errorString = str(e)
            raise Exception(errorString.split("\n")[0])

        # TODO : this should be moved into diffcal service, handled by the new saving service
        self.restockShelves(data["calibrationTable"])

        logger.info(f"Finished executing diffraction calibration for runId: {self.runNumber}")
        data["result"] = True
        return data
