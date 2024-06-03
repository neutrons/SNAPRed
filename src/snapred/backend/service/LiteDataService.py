from typing import Any, Dict

from snapred.backend.data.DataExportService import DataExportService
from snapred.backend.data.LocalDataService import LocalDataService
from snapred.backend.recipe.GenericRecipe import LiteDataRecipe as Recipe
from snapred.backend.service.Service import Service
from snapred.meta.Config import Config
from snapred.meta.decorators.FromString import FromString
from snapred.meta.decorators.Singleton import Singleton
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName


@Singleton
class LiteDataService(Service):
    def __init__(self):
        super().__init__()
        self.registerPath("createLiteData", self.reduceLiteData)
        self.dataExportService = DataExportService()
        self.dataService = LocalDataService()
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
        runNumber: str,
        instrumentDefinition: str = None,
    ) -> Dict[Any, Any]:
        liteDataMap = self._ensureLiteDataMap()
        try:
            data = Recipe().executeRecipe(
                InputWorkspace=inputWorkspace,
                LiteDataMapWorkspace=liteDataMap,
                LiteInstrumentDefinitionFile=instrumentDefinition,
                OutputWorkspace=outputWorkspace,
            )
            path = self.dataService.getIPTS(runNumber)
            pre = "nexus.lite.prefix"
            ext = "nexus.lite.extension"
            fileName = Config[pre] + str(runNumber) + Config[ext]
            self.dataExportService.exportWorkspace(path, fileName, outputWorkspace)
        except Exception as e:
            raise e
        return data
