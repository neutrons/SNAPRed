from typing import Any, Dict, List

from snapred.backend.dao.calibration.Calibration import Calibration
from snapred.backend.dao.CrystallographicInfo import CrystallographicInfo
from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.dao.SmoothDataExcludingPeaksIngredients import SmoothDataExcludingPeaksIngredients
from snapred.backend.data.DataFactoryService import DataFactoryService
from snapred.backend.recipe.VanadiumFocussedReductionRecipe import VanadiumFocussedReductionRecipe
from snapred.backend.service.Service import Service
from snapred.meta.Config import Resource
from snapred.meta.decorators.FromString import FromString
from snapred.meta.decorators.Singleton import Singleton


@Singleton
class VanadiumFocussedReductionService(Service):
    _name = "vanadiumReduction"
    dataFactoryService = DataFactoryService()

    # register the service in ServiceFactory please!
    def __init__(self):
        super().__init__()
        self.registerPath("vanadiumReduction", self.vanadiumReduction)
        return

    def name(self):
        return self._name

    @FromString
    def vanadiumReduction(self, runs: List[RunConfig]) -> Dict[Any, Any]:
        data: Dict[Any, Any] = {}
        for run in runs:
            reductionIngredients = self.dataFactoryService.getReductionIngredients(run.runNumber)
            if run.runNumber == "48741":
                crystalInfo = CrystallographicInfo.parse_raw(
                    Resource.read("vanadiumReduction/vanadiumReduction/input_crystalInfo.json")
                )
                instrumentState = Calibration.parse_raw(
                    Resource.read("vanadiumReduction/vanadiumReduction/input_parameters.json")
                ).instrumentState
                smoothIngredients = SmoothDataExcludingPeaksIngredients(
                    crystalInfo=crystalInfo, instrumentState=instrumentState
                )
            try:
                VanadiumFocussedReductionRecipe().executeRecipe(
                    reductionIngredients=reductionIngredients, smoothIngredients=smoothIngredients
                )
            except:
                raise
        return data
