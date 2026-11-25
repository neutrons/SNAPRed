import json
from pathlib import Path
from typing import Optional

from snapred.backend.dao.ingredients import GroceryListItem
from snapred.backend.data.source.CalibrationSources import diffCalTableExtension
from snapred.backend.data.source.DataSourceKind import DataSourceKind
from snapred.backend.data.source.InstrumentDonors import InstrumentDonors
from snapred.backend.data.source.NamedDataSource import NamedDataSource
from snapred.backend.data.source.Product import Product
from snapred.backend.data.source.Resolution import Resolution
from snapred.backend.data.source.SourcedData import SourcedData
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceNameGenerator as wng

# For now, reduction pixel masks share the diffraction-calibration loader.
LOADER = "LoadCalibrationWorkspaces"


def reductionPixelMaskName(runNumber: str, timestamp: Optional[float]) -> WorkspaceName:
    """The name of a reduction pixel mask.  It does not depend on the resolution."""
    return wng.reductionPixelMask().runNumber(runNumber).timestamp(timestamp).build()


class ReductionPixelMaskSource(NamedDataSource):
    """
    The pixel mask of a reduction, from the reduction record of the item's run number and timestamp.

    Native and lite mode share this implementation: the resolution only selects which reduction record is found.
    """

    def __init__(self, resolution: Resolution, dataService, grocer, instrumentDonors: InstrumentDonors):
        super().__init__(provides=resolution, product=Product.REDUCTION_PIXEL_MASK)
        self.dataService = dataService
        self.grocer = grocer
        self.instrumentDonors = instrumentDonors

    @property
    def kind(self) -> DataSourceKind:
        return DataSourceKind.REDUCTION_RECORD

    def available(self, item: GroceryListItem) -> bool:  # noqa: ARG002
        # A missing file is reported by the load.
        return True

    def outputName(self, item: GroceryListItem) -> Optional[WorkspaceName]:
        return reductionPixelMaskName(item.runNumber, item.timestamp)

    def filename(self, item: GroceryListItem) -> str:
        reductionDataPath = self.dataService._constructReductionDataPath(
            item.runNumber, item.useLiteMode, item.timestamp
        )
        return str(Path(reductionDataPath) / (self.outputName(item) + diffCalTableExtension()))

    def load(self, item: GroceryListItem, outputWorkspace: WorkspaceName) -> SourcedData:
        # Unless overridden: use a cached workspace as the instrument donor.
        instrumentPropertySource, instrumentSource = (
            ("InstrumentDonor", self.instrumentDonors.fetch(item.runNumber, item.useLiteMode))
            if not item.instrumentPropertySource
            else (item.instrumentPropertySource, item.instrumentSource)
        )
        data = self.grocer.executeRecipe(
            filename=self.filename(item),
            workspace=outputWorkspace,
            loader=LOADER,
            instrumentPropertySource=instrumentPropertySource,
            instrumentSource=instrumentSource,
            # Only the mask is loaded.
            loaderArgs=json.dumps({"MaskWorkspace": outputWorkspace}),
        )
        return self._sourced(data["workspace"], data["loader"])
