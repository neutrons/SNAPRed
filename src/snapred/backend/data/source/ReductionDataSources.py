from typing import Dict, Tuple

from snapred.backend.dao.ingredients import GroceryListItem
from snapred.backend.data.source.DataCache import ResidentDataCache
from snapred.backend.data.source.DataResolver import DataResolver
from snapred.backend.data.source.DataSourceRegistry import DataSourceRegistry
from snapred.backend.data.source.GroupingSources import GroupingDataCache, GroupingFileSource
from snapred.backend.data.source.InstrumentDonors import InstrumentDonors
from snapred.backend.data.source.Product import Product
from snapred.backend.data.source.ReductionPixelMaskSource import ReductionPixelMaskSource
from snapred.backend.data.source.Resolution import Resolution
from snapred.backend.data.source.SourcedData import SourcedData
from snapred.backend.data.source.WorkspaceOperations import WorkspaceOperations


class ReductionDataSources:
    """
    The sources of the grouping workspaces and of the reduction pixel masks, their registries and resolvers, and
    their caches.

    * A grouping is reused for every run of the same state (`groupingCache`).
    * A reduction pixel mask is reused while it is resident in the ADS.
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
                registry = DataSourceRegistry(resolution, sources, product=product)
                self.registries[(product, resolution)] = registry
                self._resolvers[(product, resolution)] = DataResolver(registry, cache, workspaces)

    def resolver(self, product: Product, item: GroceryListItem) -> DataResolver:
        return self._resolvers[(product, Resolution.fromLiteMode(item.useLiteMode))]

    def fetchGrouping(self, item: GroceryListItem) -> SourcedData:
        return self.resolver(Product.GROUPING, item).resolve(item)

    def fetchReductionPixelMask(self, item: GroceryListItem) -> SourcedData:
        return self.resolver(Product.REDUCTION_PIXEL_MASK, item).resolve(item)
