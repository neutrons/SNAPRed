import os
from typing import Any, Dict, List

from snapred.backend.dao.RunConfig import RunConfig
from snapred.backend.recipe.GenericRecipe import LiteDataRecipe as Recipe
from snapred.backend.service.Service import Service
from snapred.meta.decorators.FromString import FromString
from snapred.meta.decorators.Singleton import Singleton
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName


@Singleton
class LiteDataService(Service):
    def __init__(self):
        super().__init__()
        self.registerPath("createLiteData", self.reduceLiteData)
        return

    @staticmethod
    def name():
        return "reduceLiteData"

    def _ensureLiteDataMap(self) -> str:
        from snapred.backend.data.GroceryService import GroceryService

        return GroceryService().fetchLiteDataMap()

    @FromString
    def reduceLiteData(
        self,
        inputWorkspace: WorkspaceName,
        outputWorkspace: WorkspaceName,
        instrumentDefinition: str = "",
    ) -> Dict[Any, Any]:
        liteDataMap = self._ensureLiteDataMap(self)
        try:
            data = Recipe().executeRecipe(
                InputWorkspace=inputWorkspace,
                LiteDataMapWorkspace=liteDataMap,
                LiteInstrumentDefinitionFile=instrumentDefinition,
                OutputWorkspace=outputWorkspace,
            )
        except Exception as e:
            raise e
        return data
