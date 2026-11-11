from typing import Dict, List

from snapred.backend.data.source.DataSource import DataSource
from snapred.backend.data.source.DataSourceKind import DataSourceKind
from snapred.backend.data.source.Resolution import Resolution
from snapred.meta.Config import Config


class DataSourceRegistry:
    """
    The sources of neutron data at one resolution.

    The order in which they are tried is set by `neutronData.sources.<resolution>`.
    """

    def __init__(self, resolution: Resolution, sources: List[DataSource]):
        for source in sources:
            if source.provides != resolution:
                raise RuntimeError(
                    f"implementation error: the '{source.kind}' source provides {source.provides} data, "
                    f"not {resolution} data"
                )
        self.resolution = resolution
        self.sources: Dict[DataSourceKind, DataSource] = {source.kind: source for source in sources}

    def ordered(self) -> List[DataSource]:
        """The sources, in the configured order."""
        order = []
        for kind in Config[f"neutronData.sources.{self.resolution}"]:
            source = self.sources.get(DataSourceKind(kind))
            if source is None:
                raise RuntimeError(
                    f"configuration error: 'neutronData.sources.{self.resolution}' lists '{kind}', "
                    f"which is not a source of {self.resolution} data"
                )
            order.append(source)
        return order
