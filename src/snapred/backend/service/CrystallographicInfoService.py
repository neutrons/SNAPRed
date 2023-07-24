from typing import Any, Dict

from snapred.backend.data.DataFactoryService import DataFactoryService
from snapred.backend.recipe.CrystallographicInfoRecipe import CrystallographicInfoRecipe
from snapred.backend.service.Service import Service
from snapred.meta.decorators.FromString import FromString
from snapred.meta.decorators.Singleton import Singleton


@Singleton
class CrystallographicInfoService(Service):
    dataFactoryService = "DataFactoryService"

    # register the service in ServiceFactory please!
    def __init__(self):
        super().__init__()
        self.dataFactoryService = DataFactoryService()
        self.registerPath("", self.ingest)
        return

    @staticmethod
    def name():
        return "ingestion"

    @FromString
    def ingest(self, cifPath: str) -> Dict[Any, Any]:
        data: Dict[Any, Any] = {}
        # TODO: collect runs by state then by calibration of state, execute sets of runs by calibration of thier state
        try:
            data = CrystallographicInfoRecipe().executeRecipe(cifPath)
        except:
            raise
        return data
