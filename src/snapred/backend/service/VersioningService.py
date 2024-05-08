from pathlib import Path
from typing import Any, Dict, List, Set

from snapred.backend.recipe.GenericRecipe import LiteDataRecipe as Recipe
from snapred.backend.service.Service import Service
from snapred.meta.Config import Config
from snapred.meta.decorators.FromString import FromString
from snapred.meta.decorators.Singleton import Singleton
from snapred.meta.mantid.WorkspaceNameGenerator import ValueFormatter as wnvf
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName


@Singleton
class VersioningService(Service):
    # starting version number -- the first run printed
    VERSION_START = Config["instrument.startingVersionNumber"]

    def __init__(self):
        self.path_prefix: Path
        self.all_versions: Set = set(self.VERSION_START)
        self.all_dirs: Set = set("")

        super().__init__()
        self.registerPath("currentVersion", self.currentVersion)
        self.registerPath("nextVersion", self.nextVersion)
        return

    def file_pattern(x: int) -> str:
        return wnvf.formatVersion(x, wnvf.vPrefix.FILE)

    def name_pattern(x: int) -> str:
        return wnvf.formatVersion(x, wnvf.vPrefix.WORKSPACE)

    @staticmethod
    def name():
        return "version"

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
