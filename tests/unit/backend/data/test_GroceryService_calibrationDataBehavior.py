"""
Behavior tests for the calibration and normalization fetches of `GroceryService`.

These tests exercise only `fetchGroceryList`, and observe only:

* the returned workspace names;
* the workspaces in the ADS, and their contents.

The calibration and normalization records and files are real, in a temporary directory; only the indexers
are replaced, to report those records.  The instrument donor is given by each item, so that no neutron data
is loaded.  These tests do not depend on any private attribute or method of `GroceryService`, so that its
internal structure can be changed without changing them.
"""

from pathlib import Path
from types import SimpleNamespace

import pytest
from mantid.dataobjects import MaskWorkspace
from mantid.simpleapi import (
    CreateSampleWorkspace,
    DeleteWorkspace,
    LoadInstrument,
    SaveDiffCal,
    SaveNexusProcessed,
    mtd,
)
from util.Config_helpers import Config_override
from util.dao import DAOFactory
from util.helpers import createCompatibleDiffCalTable, createCompatibleMask
from util.instrument_helpers import addInstrumentLogs, getInstrumentLogDescriptors
from util.WhateversInTheFridge import WhateversInTheFridge

import snapred.backend.recipe.algorithm  # noqa: F401
from snapred.backend.dao.ingredients.GroceryListItem import GroceryListItem
from snapred.backend.data.GroceryService import GroceryService
from snapred.meta.Config import Config, Resource
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceNameGenerator as wng
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceType

STATE = "0123456789abcdef"
SAMPLE_RUN = "57482"
DIFFCAL_RUN = "57400"
NORMCAL_RUN = "57401"
VERSION = 3
OTHER_VERSION = 4
DIFFCAL_EXTENSION = ".h5"


def tableName(version: int = VERSION) -> str:
    return wng.diffCalTable().runNumber(DIFFCAL_RUN).version(version).build()


def maskName(version: int = VERSION) -> str:
    return wng.diffCalMask().runNumber(DIFFCAL_RUN).version(version).build()


def normalizationName(version: int = VERSION, hidden: bool = False) -> str:
    return wng.rawVanadium().runNumber(NORMCAL_RUN).version(version).hidden(hidden).build()


@pytest.fixture(scope="module")
def sampleData():
    """An event workspace, a diffraction-calibration table and a mask, on the fake SNAP instrument."""
    donor = mtd.unique_hidden_name()
    CreateSampleWorkspace(OutputWorkspace=donor, WorkspaceType="Event", XUnit="TOF", NumBanks=4, BankPixelWidth=2)
    LoadInstrument(
        Workspace=donor,
        Filename=Resource.getPath("inputs/testInstrument/fakeSNAP_Definition.xml"),
        RewriteSpectraMap=True,
    )
    addInstrumentLogs(donor, **getInstrumentLogDescriptors(DAOFactory.real_detector_state))
    table = mtd.unique_hidden_name()
    createCompatibleDiffCalTable(table, donor)
    mask = mtd.unique_hidden_name()
    createCompatibleMask(mask, donor)
    pixelCount = mtd[donor].getInstrument().getNumberDetectors(True)

    yield SimpleNamespace(donor=donor, table=table, mask=mask, pixelCount=pixelCount)

    for ws in (donor, table, mask):
        if mtd.doesExist(ws):
            DeleteWorkspace(ws)


class FakeIndexer:
    """The records of one kind of calibration, at one resolution, in a directory."""

    def __init__(self, root: Path):
        self.root = root
        self.records = {}

    def versionPath(self, version: int) -> Path:
        path = self.root / f"v_{version:04d}"
        path.mkdir(parents=True, exist_ok=True)
        return path

    def readRecord(self, version: int):
        return self.records.get(version)

    def latestApplicableVersion(self, runNumber: str):  # noqa: ARG002
        return max(self.records) if self.records else None


class Harness:
    """A `GroceryService`, with the calibration and normalization indexers replaced."""

    def __init__(self, sampleData, root: Path, monkeypatch):
        self.sampleData = sampleData
        self.root = root
        self.indexers = {
            (kind, useLiteMode): FakeIndexer(root / kind / ("lite" if useLiteMode else "native"))
            for kind in ("diffraction", "normalization")
            for useLiteMode in (False, True)
        }
        fridge = WhateversInTheFridge()
        self.service = GroceryService(dataService=fridge)
        # One state only: the state is not used.
        monkeypatch.setattr(
            fridge, "calibrationIndexer", lambda useLiteMode, **_: self._indexer("diffraction", useLiteMode)
        )
        monkeypatch.setattr(
            fridge, "normalizationIndexer", lambda useLiteMode, *_: self._indexer("normalization", useLiteMode)
        )
        instrumentState = DAOFactory.calibrationParameters().instrumentState
        monkeypatch.setattr(fridge, "generateInstrumentState", lambda _runNumber: instrumentState)

    def _indexer(self, kind: str, useLiteMode: bool) -> FakeIndexer:
        return self.indexers[(kind, useLiteMode)]

    def addDiffCal(self, useLiteMode: bool = False, version: int = VERSION, withMask: bool = True) -> Path:
        indexer = self.indexers[("diffraction", useLiteMode)]
        workspaces = {WorkspaceType.DIFFCAL_TABLE: [tableName(version)]}
        if withMask:
            workspaces[WorkspaceType.DIFFCAL_MASK] = [maskName(version)]
        indexer.records[version] = SimpleNamespace(runNumber=DIFFCAL_RUN, version=version, workspaces=workspaces)
        path = indexer.versionPath(version) / (tableName(version) + DIFFCAL_EXTENSION)
        SaveDiffCal(
            CalibrationWorkspace=self.sampleData.table,
            MaskWorkspace=self.sampleData.mask,
            Filename=str(path),
        )
        return path

    def addNormalization(self, useLiteMode: bool = False, version: int = VERSION) -> Path:
        indexer = self.indexers[("normalization", useLiteMode)]
        indexer.records[version] = SimpleNamespace(runNumber=NORMCAL_RUN, version=version)
        path = indexer.versionPath(version) / (
            normalizationName(version) + Config["calibration.normalization.output.ws.extension"]
        )
        SaveNexusProcessed(InputWorkspace=self.sampleData.donor, Filename=str(path))
        return path

    def item(self, workspaceType: str, useLiteMode: bool = False, **kwargs) -> GroceryListItem:
        return GroceryListItem(
            workspaceType=workspaceType,
            runNumber=SAMPLE_RUN,
            useLiteMode=useLiteMode,
            state=STATE,
            instrumentPropertySource="InstrumentDonor",
            instrumentSource=self.sampleData.donor,
            **kwargs,
        )

    def fetch(self, item: GroceryListItem) -> str:
        [workspace] = self.service.fetchGroceryList([item])
        return workspace


def calibrationWorkspaces() -> set:
    return {
        name
        for name in mtd.getObjectNames()
        if DIFFCAL_RUN in name or NORMCAL_RUN in name or name.startswith("explicit_diffcal")
    }


def clearCalibrationWorkspaces():
    for ws in calibrationWorkspaces() | {normalizationName(v, True) for v in (VERSION, OTHER_VERSION)}:
        if mtd.doesExist(ws):
            DeleteWorkspace(ws)


@pytest.fixture
def harness(sampleData, tmp_path, monkeypatch):
    clearCalibrationWorkspaces()
    with (
        Config_override("instrument.native.pixelResolution", sampleData.pixelCount),
        Config_override("instrument.lite.pixelResolution", sampleData.pixelCount),
        # The fetched normalization is validated against the instrument of its resolution.
        Config_override("instrument.native.name", "fakesnap"),
        Config_override("instrument.lite.name", "fakesnap"),
    ):
        yield Harness(sampleData, tmp_path, monkeypatch)
    clearCalibrationWorkspaces()


class TestDiffCalTable:
    @pytest.mark.parametrize("useLiteMode", [False, True])
    def test_fetch(self, harness, useLiteMode):
        harness.addDiffCal(useLiteMode)
        workspace = harness.fetch(harness.item("diffcal_table", useLiteMode, diffCalVersion=VERSION))
        assert workspace == tableName()
        assert mtd[workspace].rowCount() == harness.sampleData.pixelCount
        # Today, the mask in the same file is loaded as well.
        assert calibrationWorkspaces() == {tableName(), maskName()}

    def test_cached(self, harness):
        path = harness.addDiffCal()
        harness.fetch(harness.item("diffcal_table", diffCalVersion=VERSION))
        path.unlink()
        assert harness.fetch(harness.item("diffcal_table", diffCalVersion=VERSION)) == tableName()

    def test_resolution_selects_the_record(self, harness):
        # Only the lite-mode record exists.
        harness.addDiffCal(useLiteMode=True)
        with pytest.raises(Exception):  # noqa: PT011, B017
            harness.fetch(harness.item("diffcal_table", False, diffCalVersion=VERSION))
        assert harness.fetch(harness.item("diffcal_table", True, diffCalVersion=VERSION)) == tableName()

    def test_invalid_table(self, harness):
        harness.addDiffCal()
        with Config_override("instrument.native.pixelResolution", harness.sampleData.pixelCount + 1):
            with pytest.raises(RuntimeError, match=r"Expected 17 rows for the native resolution"):
                harness.fetch(harness.item("diffcal_table", diffCalVersion=VERSION))

    def test_no_record(self, harness):
        with pytest.raises(Exception):  # noqa: PT011, B017
            harness.fetch(harness.item("diffcal_table", diffCalVersion=VERSION))
        assert calibrationWorkspaces() == set()

    def test_explicit_file(self, harness):
        # `diffCalFilePath` overrides the indexed calibration, whether or not that exists.
        harness.addDiffCal()
        path = harness.root / "explicit_diffcal.h5"
        SaveDiffCal(
            CalibrationWorkspace=harness.sampleData.table, MaskWorkspace=harness.sampleData.mask, Filename=str(path)
        )
        workspace = harness.fetch(harness.item("diffcal_table", diffCalVersion=VERSION, diffCalFilePath=path))
        assert workspace == "explicit_diffcal"
        assert mtd[workspace].rowCount() == harness.sampleData.pixelCount
        assert not mtd.doesExist(tableName())


class TestDiffCalMask:
    def test_fetch(self, harness):
        harness.addDiffCal()
        workspace = harness.fetch(harness.item("diffcal_mask", diffCalVersion=VERSION))
        assert workspace == maskName()
        assert isinstance(mtd[workspace], MaskWorkspace)

    def test_cached(self, harness):
        path = harness.addDiffCal()
        harness.fetch(harness.item("diffcal_mask", diffCalVersion=VERSION))
        path.unlink()
        assert harness.fetch(harness.item("diffcal_mask", diffCalVersion=VERSION)) == maskName()

    def test_record_without_a_mask(self, harness):
        harness.addDiffCal(withMask=False)
        assert harness.fetch(harness.item("diffcal_mask", diffCalVersion=VERSION)) == ""

    def test_explicit_file(self, harness):
        path = harness.root / "explicit_diffcal.h5"
        SaveDiffCal(
            CalibrationWorkspace=harness.sampleData.table, MaskWorkspace=harness.sampleData.mask, Filename=str(path)
        )
        workspace = harness.fetch(harness.item("diffcal_mask", diffCalVersion=VERSION, diffCalFilePath=path))
        assert workspace == "explicit_diffcal_mask"
        assert isinstance(mtd[workspace], MaskWorkspace)


class TestNormalization:
    @pytest.mark.parametrize("useLiteMode", [False, True])
    def test_fetch(self, harness, useLiteMode):
        harness.addNormalization(useLiteMode)
        workspace = harness.fetch(harness.item("normalization", useLiteMode, normCalVersion=VERSION))
        assert workspace == normalizationName()
        # The normalization data is processed: its events are cropped to the instrument's TOF bounds.
        bounds = DAOFactory.calibrationParameters().instrumentState.particleBounds.tof
        assert mtd[workspace].getTofMin() >= bounds.minimum
        assert mtd[workspace].getTofMax() <= bounds.maximum

    def test_hidden(self, harness):
        harness.addNormalization()
        workspace = harness.fetch(harness.item("normalization", normCalVersion=VERSION, hidden=True))
        assert workspace == normalizationName(hidden=True)
        assert mtd.doesExist(workspace)

    def test_cached(self, harness):
        path = harness.addNormalization()
        harness.fetch(harness.item("normalization", normCalVersion=VERSION))
        path.unlink()
        assert harness.fetch(harness.item("normalization", normCalVersion=VERSION)) == normalizationName()

    def test_only_one_is_kept(self, harness):
        harness.addNormalization(version=VERSION)
        harness.addNormalization(version=OTHER_VERSION)
        harness.fetch(harness.item("normalization", normCalVersion=VERSION))
        harness.fetch(harness.item("normalization", normCalVersion=OTHER_VERSION))
        assert mtd.doesExist(normalizationName(OTHER_VERSION))
        assert not mtd.doesExist(normalizationName(VERSION))

    def test_diffcal_is_applied(self, harness):
        harness.addNormalization()
        harness.addDiffCal()
        workspace = harness.fetch(harness.item("normalization", normCalVersion=VERSION, diffCalVersion=VERSION))
        assert mtd.doesExist(workspace)
        assert mtd.doesExist(tableName())

    def test_no_record(self, harness):
        with pytest.raises(RuntimeError, match=r"Could not find normalization record"):
            harness.fetch(harness.item("normalization", normCalVersion=VERSION))
