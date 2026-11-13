import json
from pathlib import Path
from typing import Optional, Tuple

from snapred.backend.dao.indexing.Versioning import VERSION_START, Version, VersionState
from snapred.backend.dao.ingredients import GroceryListItem
from snapred.backend.data.source.DataSourceKind import DataSourceKind
from snapred.backend.data.source.InstrumentDonors import InstrumentDonors
from snapred.backend.data.source.NamedDataSource import NamedDataSource
from snapred.backend.data.source.Product import Product
from snapred.backend.data.source.Resolution import Resolution
from snapred.backend.data.source.SourcedData import SourcedData
from snapred.backend.data.source.WorkspaceOperations import WorkspaceOperations
from snapred.backend.log.logger import snapredLogger
from snapred.meta.Config import Config
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceName, WorkspaceType
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceNameGenerator as wng

logger = snapredLogger.getLogger(__name__)

LOADER = "LoadCalibrationWorkspaces"
CALIBRATION_PRODUCTS = (Product.DIFFCAL_TABLE, Product.DIFFCAL_MASK)


def diffCalTableName(diffcalRunNumber: str, version: Optional[Version]) -> WorkspaceName:
    """The name of a diffraction-calibration table.  The default version has its own name, whatever the run."""
    if version in [VersionState.DEFAULT, VERSION_START()]:
        return wng.diffCalTable().runNumber("default").version(VersionState.DEFAULT).build()
    return wng.diffCalTable().runNumber(diffcalRunNumber).version(version).build()


def diffCalTableExtension() -> str:
    return Config["calibration.diffraction.table.extension"]


def validateCalibrationTable(workspaces: WorkspaceOperations, useLiteMode: bool, tableWorkspaceName: str):
    """A calibration table must have one row per pixel of the instrument, and positive 'difc' values."""
    tableInst = workspaces.getWorkspaceForName(tableWorkspaceName)
    if tableInst is None:
        raise RuntimeError(f"Calibration table workspace '{tableWorkspaceName}' does not exist in the ADS.")

    totalExpectedRows = (
        Config["instrument.lite.pixelResolution"] if useLiteMode else Config["instrument.native.pixelResolution"]
    )
    if len(tableInst.column("detid")) != totalExpectedRows:
        raise RuntimeError(
            f"Calibration table workspace '{tableWorkspaceName}' has {len(tableInst.column('detid'))} rows, "
            f"\nExpected {totalExpectedRows} rows for the {'lite' if useLiteMode else 'native'} resolution."
            f"\ni.e. one row per pixel of the instrument."
        )
    # ensure all difc's are positive floats
    if not all(isinstance(difc, float) and difc > 0 for difc in tableInst.column("difc")):
        raise RuntimeError(
            f"Calibration table workspace '{tableWorkspaceName}' has invalid 'difc' values. "
            "All 'difc' values must be positive floats."
        )


class CalibrationWorkspaceSource(NamedDataSource):
    """
    A diffraction-calibration table, or its mask, from a calibration file.

    The table and the mask are in the same file, but each is a separate product, with its own source:
    `LoadCalibrationWorkspaces` is asked for the one output only.  Native and lite mode share this
    implementation: the resolution only selects which file is found.
    """

    def __init__(
        self,
        product: Product,
        resolution: Resolution,
        grocer,
        workspaces: WorkspaceOperations,
        instrumentDonors: InstrumentDonors,
    ):
        if product not in CALIBRATION_PRODUCTS:
            raise RuntimeError(f"implementation error: '{product}' is not a diffraction-calibration product")
        super().__init__(provides=resolution, product=product)
        self.grocer = grocer
        self.workspaces = workspaces
        self.instrumentDonors = instrumentDonors

    @property
    def isTable(self) -> bool:
        return self.product == Product.DIFFCAL_TABLE

    def _loadFromFile(self, item: GroceryListItem, filename: str, workspace: WorkspaceName) -> SourcedData:
        # Unless overridden: use a cached workspace as the instrument donor.
        instrumentPropertySource, instrumentSource = (
            ("InstrumentDonor", self.instrumentDonors.fetch(item.runNumber, item.useLiteMode))
            if not item.instrumentPropertySource
            else (item.instrumentPropertySource, item.instrumentSource)
        )
        logger.debug(f"Loading {self.product} from {filename} for run {item.runNumber}, lite mode {item.useLiteMode}")
        data = self.grocer.executeRecipe(
            filename=filename,
            # The 'workspace' property must be a `MatrixWorkspace`: so it is the mask, or nothing.
            workspace="" if self.isTable else workspace,
            loader=LOADER,
            instrumentPropertySource=instrumentPropertySource,
            instrumentSource=instrumentSource,
            loaderArgs=json.dumps(
                {
                    "CalibrationTable": workspace if self.isTable else "",
                    "MaskWorkspace": "" if self.isTable else workspace,
                }
            ),
        )
        if self.isTable:
            validateCalibrationTable(self.workspaces, item.useLiteMode, workspace)
        return self._sourced(workspace, data["loader"])


class CalibrationRecordSource(CalibrationWorkspaceSource):
    """The table, or the mask, of the calibration version that the item names, from its calibration record."""

    def __init__(self, product: Product, resolution: Resolution, dataService, grocer, workspaces, instrumentDonors):
        super().__init__(product, resolution, grocer, workspaces, instrumentDonors)
        self.dataService = dataService

    @property
    def kind(self) -> DataSourceKind:
        return DataSourceKind.CALIBRATION_RECORD

    def available(self, item: GroceryListItem) -> bool:  # noqa: ARG002
        # A missing record is reported by the lookup.
        return True

    @staticmethod
    def _firstWorkspaceOfType(workspaces, wsType: WorkspaceType) -> Optional[WorkspaceName]:
        # `CalibrationRecord.workspaces: Dict[WorkspaceType, List[WorkspaceName]]`
        names = next((names for key, names in workspaces.items() if key == wsType), None)
        return names[0] if names else None

    def _lookup(self, item: GroceryListItem) -> Tuple[int, str, WorkspaceName, Optional[WorkspaceName]]:
        """The version, the calibration run number, and the table and mask names in the record."""
        indexer = self.dataService.calibrationIndexer(item.useLiteMode, state=item.state)
        version = item.diffCalVersion
        if not isinstance(version, int):
            version = indexer.latestApplicableVersion(item.runNumber)
        record = indexer.readRecord(version)
        if record is None:
            raise RuntimeError(f"Could not find calibration record for run {item.runNumber} and version {version}")
        table = self._firstWorkspaceOfType(record.workspaces, WorkspaceType.DIFFCAL_TABLE)
        if table is None:
            raise RuntimeError(f"Could not find any table workspaces in calibration record for run: '{item.runNumber}'")
        # A mask workspace is optional.
        mask = self._firstWorkspaceOfType(record.workspaces, WorkspaceType.DIFFCAL_MASK)
        return version, record.runNumber, table, mask

    def outputName(self, item: GroceryListItem) -> Optional[WorkspaceName]:
        _, _, table, mask = self._lookup(item)
        return table if self.isTable else mask

    def load(self, item: GroceryListItem, outputWorkspace: WorkspaceName) -> SourcedData:
        version, diffcalRunNumber, table, _ = self._lookup(item)
        indexer = self.dataService.calibrationIndexer(item.useLiteMode, state=item.state)
        expectedTable = diffCalTableName(diffcalRunNumber, version)
        if table != expectedTable:
            raise ValueError(
                f"Workspace name {table} does not match the expected diffcal table workspace name "
                f"for run {diffcalRunNumber} (i.e. {expectedTable}), path: {indexer.versionPath(version)}"
            )
        # The table and the mask are in the same file, named for the table.
        filename = Path(indexer.versionPath(version)) / (table + diffCalTableExtension())
        return self._loadFromFile(item, str(filename), outputWorkspace)


class CalibrationFileSource(CalibrationWorkspaceSource):
    """
    The table, or the mask, from the calibration file that the item names (`diffCalFilePath`).

    It is exclusive: when the item names a file, no calibration record is used.  The table is named for the
    file, and the mask for the table.
    """

    MASK_SUFFIX = "_mask"

    @property
    def kind(self) -> DataSourceKind:
        return DataSourceKind.CALIBRATION_FILE

    def exclusiveFor(self, item: GroceryListItem) -> bool:
        return item.diffCalFilePath is not None

    def available(self, item: GroceryListItem) -> bool:
        return self.exclusiveFor(item)

    def outputName(self, item: GroceryListItem) -> Optional[WorkspaceName]:
        table = Path(item.diffCalFilePath).stem
        return table if self.isTable else f"{table}{self.MASK_SUFFIX}"

    def load(self, item: GroceryListItem, outputWorkspace: WorkspaceName) -> SourcedData:
        return self._loadFromFile(item, str(Path(item.diffCalFilePath).resolve()), outputWorkspace)
