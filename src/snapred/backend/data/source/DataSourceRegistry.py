from typing import Dict, List

from snapred.backend.data.source.DataSource import DataSource
from snapred.backend.data.source.DataSourceKind import DataSourceKind
from snapred.backend.data.source.Product import Product
from snapred.backend.data.source.Resolution import Resolution
from snapred.meta.Config import Config


class DataSourceRegistry:
    """
    The sources of one product at one resolution.

    The order in which they are tried is set by `dataSources.<product>.<resolution>`.
    """

    def __init__(self, resolution: Resolution, sources: List[DataSource], product: Product = Product.NEUTRON):
        for source in sources:
            if source.provides != resolution or source.product != product:
                raise RuntimeError(
                    f"implementation error: the '{source.kind}' source provides {source.provides} "
                    f"{source.product} data, not {resolution} {product} data"
                )
        self.resolution = resolution
        self.product = product
        self.sources: Dict[DataSourceKind, DataSource] = {source.kind: source for source in sources}

    def ordered(self) -> List[DataSource]:
        """The sources, in the configured order."""
        order = []
        key = f"dataSources.{self.product}.{self.resolution}"
        for kind in Config[key]:
            source = self.sources.get(DataSourceKind(kind))
            if source is None:
                raise RuntimeError(
                    f"configuration error: '{key}' lists '{kind}', "
                    f"which is not a source of {self.resolution} {self.product} data"
                )
            order.append(source)
        return order
