import os
from typing import Any, Dict, List

from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.data.DataFactoryService import DataFactoryService
from snapred.backend.recipe.LiteDataRecipe import LiteDataRecipe
from snapred.backend.service.Service import Service
from snapred.meta.decorators.FromString import FromString
from snapred.meta.decorators.Singleton import Singleton


@Singleton
class LiteDataService(Service):
    dataFactoryService = "DataFactoryService"

    def __init__(self):
        super().__init__()
        self.dataFactoryService = DataFactoryService
        self.registerPath("", self.reduceLiteData)
        return

    @staticmethod
    def name():
        return "reduceLiteData"

    @FromString
    def reduceLiteData(self, runs: List[RunConfig]) -> Dict[Any, Any]:
        pass
