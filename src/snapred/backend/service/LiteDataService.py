import os
from typing import Any, Dict, List

from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.data.DataFactoryService import DataFactoryService
from snapred.backend.recipe.GenericRecipe import LiteDataRecipe as Recipe
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
        data: Dict[Any, Any] = {}
        for run in runs:
            inputWorkspace = "SNAP_" + str(run.runNumber) + ".nxs"
            try:
                data[str(run.runNumber)] = Recipe().executeRecipe(inputWorkspace=inputWorkspace, runNumber=run.runNumber)
            except Exception as e:
                raise e
        return data
