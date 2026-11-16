from pathlib import Path
from typing import Optional

from snapred.backend.dao.ingredients import GroceryListItem
from snapred.backend.data.source.DataSourceKind import DataSourceKind
from snapred.backend.data.source.NamedDataSource import NamedDataSource
from snapred.backend.data.source.Product import Product
from snapred.backend.data.source.Resolution import Resolution
from snapred.backend.data.source.SourcedData import SourcedData
from snapred.meta.Config import Config
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceNameGenerator as wng


class NormalizationRecordSource(NamedDataSource):
    """
    The normalization of the version that the item names, from its normalization record.

    Native and lite mode share this implementation: the resolution only selects which file is found.
    The processing of the loaded data is a separate step, which is not part of the source.
    """

    def __init__(self, resolution: Resolution, dataService, grocer):
        super().__init__(provides=resolution, product=Product.NORMALIZATION)
        self.dataService = dataService
        self.grocer = grocer

    @property
    def kind(self) -> DataSourceKind:
        return DataSourceKind.NORMALIZATION_RECORD

    def available(self, item: GroceryListItem) -> bool:  # noqa: ARG002
        # A missing record is reported by the lookup.
        return True

    def _normcalRunNumber(self, item: GroceryListItem) -> str:
        record = self.dataService.normalizationIndexer(item.useLiteMode, item.state).readRecord(item.normCalVersion)
        if record is None:
            raise RuntimeError(
                f"Could not find normalization record for run {item.runNumber} and version {item.normCalVersion}"
            )
        return record.runNumber

    @staticmethod
    def _name(normcalRunNumber: str, version: Optional[int], hidden: bool) -> WorkspaceName:
        return wng.rawVanadium().runNumber(normcalRunNumber).version(version).hidden(hidden).build()

    def outputName(self, item: GroceryListItem) -> Optional[WorkspaceName]:
        return self._name(self._normcalRunNumber(item), item.normCalVersion, item.hidden)

    def filePath(self, item: GroceryListItem) -> Path:
        # NOTE: The normalization record does not store workspace type -> workspace name mappings:
        #   the file is named for the workspace, as it is when it is not hidden.
        version = item.normCalVersion
        directory = Path(self.dataService.normalizationIndexer(item.useLiteMode, item.state).versionPath(version))
        name = self._name(self._normcalRunNumber(item), version, False)
        return directory / (name + Config["calibration.normalization.output.ws.extension"])

    def load(self, item: GroceryListItem, outputWorkspace: WorkspaceName) -> SourcedData:
        # Unfortunately, `LoadNexusProcessed` does not support the `NumberOfBins=1` optimization for loading
        #   event data.  As a work-around: event-format normalization data will be saved with only one bin
        #   to speed up reload.  (See also:  "nexus.dataFormat.event" flag in "application.yml".)
        # Note: 'LoadNexusProcessed' neither requires nor makes use of an instrument donor.
        data = self.grocer.executeRecipe(
            filename=str(self.filePath(item)), workspace=outputWorkspace, loader=item.loader
        )
        return self._sourced(outputWorkspace, data["loader"])
