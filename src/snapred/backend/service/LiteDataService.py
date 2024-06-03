from typing import Any, Dict

from snapred.backend.data.DataExportService import DataExportService
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
        self.dataExportService = DataExportService()
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
        instrumentDefinition: str = None,
    ) -> Dict[Any, Any]:
        liteDataMap = self._ensureLiteDataMap()
        runNumber = inputWorkspace.split("_")[-1].lstrip("0")
        try:
            data = Recipe().executeRecipe(
                InputWorkspace=inputWorkspace,
                LiteDataMapWorkspace=liteDataMap,
                LiteInstrumentDefinitionFile=instrumentDefinition,
                OutputWorkspace=outputWorkspace,
            )
            fullPath = self.dataExportService.getFullLiteDataFilePath(runNumber)
            path = fullPath.rsplit("/", 1)[0]
            fileName = fullPath.rsplit("/", 1)[1]
            self.dataExportService.exportWorkspace(path, fileName, outputWorkspace)
        except Exception as e:
            raise e
        return data
