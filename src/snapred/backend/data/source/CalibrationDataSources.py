from typing import Dict, Optional, Tuple

from snapred.backend.dao.ingredients import GroceryListItem
from snapred.backend.data.source.CalibrationSources import CalibrationFileSource, CalibrationRecordSource
from snapred.backend.data.source.DataCache import ResidentDataCache, SingleEntryDataCache
from snapred.backend.data.source.DataResolver import DataResolver
from snapred.backend.data.source.DataSourceRegistry import DataSourceRegistry
from snapred.backend.data.source.InstrumentDonors import InstrumentDonors
from snapred.backend.data.source.NormalizationSource import NormalizationRecordSource
from snapred.backend.data.source.Product import Product
from snapred.backend.data.source.Resolution import Resolution
from snapred.backend.data.source.SourcedData import SourcedData
from snapred.backend.data.source.WorkspaceOperations import WorkspaceOperations


class CalibrationDataSources:
    """
    The sources of the diffraction-calibration table, the diffraction-calibration mask and the normalization,
    their registries and resolvers, and their caches.

    * The calibration table and mask are reused while they are resident in the ADS.
    * Only the last normalization that was loaded is kept (`normalizationCache`).
    """

    def __init__(self, dataService, grocer, workspaces: WorkspaceOperations, instrumentDonors: InstrumentDonors):
        resident = ResidentDataCache(workspaces)
        self.normalizationCache = SingleEntryDataCache(workspaces)

        def calibrationSources(product: Product, resolution: Resolution):
            return [
                CalibrationFileSource(product, resolution, grocer, workspaces, instrumentDonors),
                CalibrationRecordSource(product, resolution, dataService, grocer, workspaces, instrumentDonors),
            ]

        self.registries: Dict[Tuple[Product, Resolution], DataSourceRegistry] = {}
        self._resolvers: Dict[Tuple[Product, Resolution], DataResolver] = {}
        for resolution in Resolution:
            for product, sources, cache in (
                (Product.DIFFCAL_TABLE, calibrationSources(Product.DIFFCAL_TABLE, resolution), resident),
                (Product.DIFFCAL_MASK, calibrationSources(Product.DIFFCAL_MASK, resolution), resident),
                (
                    Product.NORMALIZATION,
                    [NormalizationRecordSource(resolution, dataService, grocer)],
                    self.normalizationCache,
                ),
            ):
                registry = DataSourceRegistry(resolution, sources, product=product)
                self.registries[(product, resolution)] = registry
                self._resolvers[(product, resolution)] = DataResolver(registry, cache, workspaces)

    def resolver(self, product: Product, item: GroceryListItem) -> DataResolver:
        return self._resolvers[(product, Resolution.fromLiteMode(item.useLiteMode))]

    def fetchDiffCalTable(self, item: GroceryListItem) -> SourcedData:
        return self.resolver(Product.DIFFCAL_TABLE, item).resolve(item)

    def fetchDiffCalMask(self, item: GroceryListItem) -> Optional[SourcedData]:
        """The mask, or `None` if the calibration has no mask."""
        return self.resolver(Product.DIFFCAL_MASK, item).resolve(item)

    def fetchNormalization(self, item: GroceryListItem) -> SourcedData:
        return self.resolver(Product.NORMALIZATION, item).resolve(item)
