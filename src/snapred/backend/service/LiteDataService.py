from typing import Any, Dict

from snapred.backend.dao.request import FarmFreshIngredients
from snapred.backend.data.DataExportService import DataExportService
from snapred.backend.data.DataFactoryService import DataFactoryService
from snapred.backend.recipe.GenericRecipe import LiteDataRecipe as Recipe
from snapred.backend.service.Service import Service
from snapred.backend.service.SousChef import SousChef
from snapred.meta.decorators.FromString import FromString
from snapred.meta.decorators.Singleton import Singleton
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName


@Singleton
class LiteDataService(Service):
    def __init__(self):
        super().__init__()
        self.registerPath("createLiteData", self.createLiteData)
        self.dataExportService = DataExportService()
        self.dataFactoryService = DataFactoryService()
        self.sousChef = SousChef()
        return

    @staticmethod
    def name():
        return "createLiteData"

    def _ensureLiteDataMap(self) -> str:
        from snapred.backend.data.GroceryService import GroceryService

        return GroceryService().fetchLiteDataMap()

    @FromString
    def createLiteData(
        self,
        inputWorkspace: WorkspaceName,
        outputWorkspace: WorkspaceName,
        instrumentDefinition: str = None,
        export: bool = True
    ) -> Dict[Any, Any]:
        liteDataMap = self._ensureLiteDataMap()
        runNumber = inputWorkspace.split("_")[-1].lstrip("0")

        ffIngredients = FarmFreshIngredients(runNumber=runNumber, useLiteMode=True)
        ingredients = self.sousChef.prepInstrumentState(ffIngredients)
        try:
            data = Recipe().executeRecipe(
                InputWorkspace=inputWorkspace,
                LiteDataMapWorkspace=liteDataMap,
                LiteInstrumentDefinitionFile=instrumentDefinition,
                OutputWorkspace=outputWorkspace,
                Ingredients=ingredients.model_dump_json(),
            )
            if export:
                # Export the converted data to disk
                fullPath = self.dataExportService.getFullLiteDataFilePath(runNumber)
                path = fullPath.parent
                fileName = fullPath.name
                self.dataExportService.exportWorkspace(path, fileName, outputWorkspace)
        except Exception as e:
            raise e
        return data
