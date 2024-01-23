from typing import Any, Dict

from snapred.backend.recipe.CrystallographicInfoRecipe import CrystallographicInfoRecipe
from snapred.backend.service.Service import Service
from snapred.meta.Config import Config
from snapred.meta.decorators.FromString import FromString
from snapred.meta.decorators.Singleton import Singleton


@Singleton
class CrystallographicInfoService(Service):
    D_MIN = Config["constants.CrystallographicInfo.dMin"]
    D_MAX = Config["constants.CrystallographicInfo.dMax"]

    # register the service in ServiceFactory please!
    # NEVER!
    def __init__(self):
        super().__init__()
        self.registerPath("", self.ingest)
        return

    @staticmethod
    def name():
        return "ingestion"

    @FromString
    def ingest(self, cifPath: str, dMin: float = D_MIN, dMax: float = D_MAX) -> Dict[Any, Any]:
        data: Dict[Any, Any] = {}
        # TODO: collect runs by state then by calibration of state, execute sets of runs by calibration of thier state
        try:
            data = CrystallographicInfoRecipe().executeRecipe(cifPath=cifPath, dMin=dMin, dMax=dMax)
        except:
            raise
        return data
