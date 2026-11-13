from typing import Any, Dict

from snapred.backend.dao.ingredients import GroceryListItem
from snapred.backend.data.source.DataSourceRegistry import DataSourceRegistry
from snapred.backend.data.source.FileSource import FileSource
from snapred.backend.data.source.LiteConversionSource import LiteConversionSource
from snapred.backend.data.source.LiveListenerSource import LiveListenerSource
from snapred.backend.data.source.NeutronDataCache import NeutronDataCache
from snapred.backend.data.source.NeutronDataResolver import NeutronDataResolver
from snapred.backend.data.source.Resolution import Resolution
from snapred.backend.data.source.WorkspaceOperations import WorkspaceOperations
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName


class NeutronDataSources:
    """
    The sources of neutron data, their registries and resolvers, and the neutron-data cache.

    * `native`: the sources of native-resolution data.  These are used by the lite-mode conversion,
      as well as for native-resolution fetches.
    * `lite`: the sources of lite-mode data.
    """

    def __init__(self, dataService, grocer, workspaces: WorkspaceOperations):
        self.cache = NeutronDataCache(workspaces)
        self.native = DataSourceRegistry(
            Resolution.NATIVE,
            [
                FileSource(Resolution.NATIVE, dataService, grocer),
                LiveListenerSource(dataService, grocer, workspaces),
            ],
        )
        self._liteConversion = LiteConversionSource(self.native, self.cache, workspaces)
        self.lite = DataSourceRegistry(
            Resolution.LITE,
            [
                FileSource(Resolution.LITE, dataService, grocer),
                self._liteConversion,
            ],
        )
        self._resolvers: Dict[Resolution, NeutronDataResolver] = {
            sources.resolution: NeutronDataResolver(sources, self.cache, workspaces)
            for sources in (self.native, self.lite)
        }

    def resolver(self, item: GroceryListItem) -> NeutronDataResolver:
        """The resolver for the item's resolution."""
        return self._resolvers[Resolution.fromLiteMode(item.useLiteMode)]

    def fetchSingleUse(self, item: GroceryListItem) -> Dict[str, Any]:
        return self.resolver(item).fetchSingleUse(item)

    def fetchCached(self, item: GroceryListItem) -> Dict[str, Any]:
        return self.resolver(item).fetchCached(item)

    def convertToLiteMode(self, workspace: WorkspaceName, export: bool = True):
        """Convert a workspace (in place) to lite mode, and optionally export it to disk."""
        self._liteConversion.convert(workspace, export=export)
