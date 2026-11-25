from typing import Dict, Tuple

from snapred.backend.dao.ingredients import GroceryListItem
from snapred.backend.data.source.DataCache import ResidentDataCache
from snapred.backend.data.source.DataResolver import DataResolver
from snapred.backend.data.source.DataSourceRegistry import DataSourceRegistry
from snapred.backend.data.source.GroupingSources import GroupingDataCache, GroupingFileSource
from snapred.backend.data.source.InstrumentDonors import InstrumentDonors
from snapred.backend.data.source.MonitorSource import MonitorSource
from snapred.backend.data.source.Product import Product
from snapred.backend.data.source.ReductionPixelMaskSource import ReductionPixelMaskSource
from snapred.backend.data.source.Resolution import Resolution
from snapred.backend.data.source.SourcedData import SourcedData
from snapred.backend.data.source.WorkspaceOperations import WorkspaceOperations


class ReductionDataSources:
    """
    The sources of the grouping workspaces, the reduction pixel masks and the monitors, their registries and
    resolvers, and their caches.

    * A grouping is reused for every run of the same state (`groupingCache`).
    * A reduction pixel mask, or a monitor, is reused while it is resident in the ADS.
    * A monitor has no lite-mode form: it has only a native registry, and is fetched at native resolution for any
      item.
    """

    def __init__(self, dataService, grocer, workspaces: WorkspaceOperations, instrumentDonors: InstrumentDonors):
        self.groupingCache = GroupingDataCache(dataService, workspaces)
        resident = ResidentDataCache(workspaces)

        self.registries: Dict[Tuple[Product, Resolution], DataSourceRegistry] = {}
        self._resolvers: Dict[Tuple[Product, Resolution], DataResolver] = {}
        for resolution in Resolution:
            for product, sources, cache in (
                (
                    Product.GROUPING,
                    [GroupingFileSource(resolution, dataService, grocer, instrumentDonors)],
                    self.groupingCache,
                ),
                (
                    Product.REDUCTION_PIXEL_MASK,
                    [ReductionPixelMaskSource(resolution, dataService, grocer, instrumentDonors)],
                    resident,
                ),
            ):
                self._add(product, resolution, sources, cache, workspaces)
        self._add(Product.MONITOR, Resolution.NATIVE, [MonitorSource(dataService, grocer)], resident, workspaces)

    def _add(self, product: Product, resolution: Resolution, sources, cache, workspaces: WorkspaceOperations):
        registry = DataSourceRegistry(resolution, sources, product=product)
        self.registries[(product, resolution)] = registry
        self._resolvers[(product, resolution)] = DataResolver(registry, cache, workspaces)

    def resolver(self, product: Product, item: GroceryListItem) -> DataResolver:
        return self._resolvers[(product, Resolution.fromLiteMode(item.useLiteMode))]

    def fetchGrouping(self, item: GroceryListItem) -> SourcedData:
        return self.resolver(Product.GROUPING, item).resolve(item)

    def fetchReductionPixelMask(self, item: GroceryListItem) -> SourcedData:
        return self.resolver(Product.REDUCTION_PIXEL_MASK, item).resolve(item)

    def fetchMonitor(self, item: GroceryListItem) -> SourcedData:
        """The monitor of the item's run, at native resolution, whatever the item's resolution."""
        nativeItem = item.model_copy(update={"useLiteMode": False})
        return self.resolver(Product.MONITOR, nativeItem).resolve(nativeItem)
