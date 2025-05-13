from typing import Any, Dict

from snapred.backend.recipe.CrystallographicInfoRecipe import CrystallographicInfoRecipe
from snapred.backend.service.Service import Service
from snapred.meta.decorators.ConfigDefault import ConfigDefault, ConfigValue
from snapred.meta.decorators.Singleton import Singleton


@Singleton
class CrystallographicInfoService(Service):
    def __init__(self):
        super().__init__()
        self.registerPath("", self.ingest)
        return

    @staticmethod
    def name():
        return "ingestion"

    @ConfigDefault
    def ingest(
        self,
        cifPath: str,
        crystalDMin: float = ConfigValue("constants.CrystallographicInfo.crystalDMin"),
        crystalDMax: float = ConfigValue("constants.CrystallographicInfo.crystalDMax"),
    ) -> Dict[Any, Any]:
        data: Dict[Any, Any] = {}
        # TODO: collect runs by state then by calibration of state, execute sets of runs by calibration of thier state
        try:
            data = CrystallographicInfoRecipe().executeRecipe(
                cifPath=cifPath, crystalDMin=crystalDMin, crystalDMax=crystalDMax
            )
        except:
            raise
        return data
