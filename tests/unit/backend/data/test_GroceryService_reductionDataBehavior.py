"""
Behavior tests for the grouping, reduction-pixel-mask and monitor fetches of `GroceryService`.

These tests exercise only `fetchGroceryList`, `fetchMonitorWorkspace`, `getCachedWorkspaces` and
`rebuildCache`, and observe only:

* the returned workspace names;
* the workspaces in the ADS, and their contents.

The grouping definitions, the reduction pixel masks and the NeXus file are real, in a temporary directory
or in the test resources; only the data-service lookups that locate them are replaced.  The instrument donor
is given by each item, so that no neutron data is loaded.  These tests do not depend on any private
attribute or method of `GroceryService`, so that its internal structure can be changed without changing them.
"""

from pathlib import Path
from types import SimpleNamespace

import h5py
import numpy as np
import pytest
from mantid.dataobjects import GroupingWorkspace, MaskWorkspace
from mantid.kernel import amend_config
from mantid.simpleapi import (
    CreateSampleWorkspace,
    DeleteWorkspace,
    LoadGroupingDefinition,
    LoadInstrument,
    SaveDiffCal,
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

RUN = "57482"
OTHER_RUN = "57483"
TIMESTAMP = 1700000000.0
OTHER_TIMESTAMP = 1700000001.0
SCHEME = "Column"
OTHER_SCHEME = "Bank"
LITE_SCHEME = "Lite"
INSTRUMENT_DEFINITION = Resource.getPath("inputs/testInstrument/fakeSNAP_Definition.xml")
GROUPING_DEFINITIONS = {
    SCHEME: Resource.getPath("inputs/testInstrument/fakeSNAPFocGroup_Column.xml"),
    OTHER_SCHEME: Resource.getPath("inputs/testInstrument/fakeSNAPFocGroup_Bank.xml"),
}
LITE_GROUPING_MAP = Resource.getPath("inputs/testInstrument/fakeSNAPLiteGroupMap.xml")


def groupingName(scheme: str = SCHEME, runNumber: str = RUN, useLiteMode: bool = False) -> str:
    if scheme == LITE_SCHEME:
        return wng.liteDataMap().build()
    resolution = "lite" if useLiteMode else "native"
    return f"__{Config['grouping.workspacename.' + resolution]}_{scheme}_{runNumber}"


def pixelMaskName(runNumber: str = RUN, timestamp: float = TIMESTAMP) -> str:
    return wng.reductionPixelMask().runNumber(runNumber).timestamp(timestamp).build()


def monitorName(runNumber: str = RUN) -> str:
    return wng.monitor().runNumber(runNumber).build()


def writeMonitorNexus(path: Path, counts: np.ndarray):
    """A minimal NeXus file with one histogram monitor, and the fake SNAP instrument embedded."""
    with h5py.File(path, "w") as f:
        entry = f.create_group("entry")
        entry.attrs["NX_class"] = "NXentry"
        entry["start_time"] = np.bytes_("2024-01-01T00:00:00")
        instrument = entry.create_group("instrument")
        instrument.attrs["NX_class"] = "NXinstrument"
        instrument["name"] = np.bytes_("fakesnap")
        xml = instrument.create_group("instrument_xml")
        xml.attrs["NX_class"] = "NXnote"
        xml["data"] = np.bytes_(Path(INSTRUMENT_DEFINITION).read_text())
        xml["type"] = np.bytes_("text/xml")
        monitor = entry.create_group("monitor1")
        monitor.attrs["NX_class"] = "NXmonitor"
        data = monitor.create_dataset("data", data=counts.astype(np.uint32))
        data.attrs["signal"] = 1
        data.attrs["axes"] = "time_of_flight"
        tof = monitor.create_dataset("time_of_flight", data=np.linspace(0.0, 1000.0, len(counts) + 1, dtype=np.float32))
        tof.attrs["units"] = "microsecond"
        monitor["distance"] = np.float32(-1.0)


@pytest.fixture(scope="module")
def sampleData():
    """An instrument donor, a diffraction-calibration table and a mask, on the fake SNAP instrument."""
    donor = mtd.unique_hidden_name()
    CreateSampleWorkspace(OutputWorkspace=donor, WorkspaceType="Event", XUnit="TOF", NumBanks=4, BankPixelWidth=2)
    LoadInstrument(Workspace=donor, Filename=INSTRUMENT_DEFINITION, RewriteSpectraMap=True)
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


class Harness:
    """
    A `GroceryService`, with the data-service lookups that locate the grouping definitions,
    the reduction data and the NeXus files replaced.  Every run number has the same state.
    """

    def __init__(self, sampleData, root: Path, monkeypatch):
        self.sampleData = sampleData
        self.root = root
        self.groupingDefinitions = dict(GROUPING_DEFINITIONS)
        self.neutronFiles = {}
        fridge = WhateversInTheFridge()
        self.service = GroceryService(dataService=fridge)
        monkeypatch.setattr(
            fridge,
            "readGroupingMap",
            lambda _runNumber: SimpleNamespace(
                getMap=lambda _useLiteMode: {
                    scheme: SimpleNamespace(definition=path) for scheme, path in self.groupingDefinitions.items()
                }
            ),
        )
        monkeypatch.setattr(fridge, "_constructReductionDataPath", self.reductionDataPath)
        monkeypatch.setattr(
            fridge, "createNeutronFilePath", lambda runNumber, _useLiteMode: self.neutronFiles.get(runNumber)
        )

    def reductionDataPath(self, runNumber: str, useLiteMode: bool, timestamp: float) -> Path:
        path = self.root / "reduction" / runNumber / ("lite" if useLiteMode else "native") / str(timestamp)
        path.mkdir(parents=True, exist_ok=True)
        return path

    def addGroupingDefinition(self, scheme: str) -> Path:
        """A copy of a grouping definition, in the temporary directory, so that it can be removed."""
        path = self.root / Path(GROUPING_DEFINITIONS[scheme]).name
        path.write_text(Path(GROUPING_DEFINITIONS[scheme]).read_text())
        self.groupingDefinitions[scheme] = str(path)
        return path

    def addPixelMask(self, runNumber: str = RUN, useLiteMode: bool = False, timestamp: float = TIMESTAMP) -> Path:
        path = self.reductionDataPath(runNumber, useLiteMode, timestamp) / (
            pixelMaskName(runNumber, timestamp) + Config["calibration.diffraction.table.extension"]
        )
        SaveDiffCal(CalibrationWorkspace=self.sampleData.table, MaskWorkspace=self.sampleData.mask, Filename=str(path))
        return path

    def addNeutronFile(self, runNumber: str = RUN, counts=None) -> Path:
        # A new file each time: Mantid may keep an earlier file of the same name open.
        path = self.root / f"SNAP_{runNumber}_{len(list(self.root.glob('SNAP_*')))}.nxs.h5"
        writeMonitorNexus(path, np.arange(10) if counts is None else np.asarray(counts))
        self.neutronFiles[runNumber] = path
        return path

    def item(self, workspaceType: str, runNumber: str = RUN, useLiteMode: bool = False, **kwargs) -> GroceryListItem:
        return GroceryListItem(
            workspaceType=workspaceType,
            runNumber=runNumber,
            useLiteMode=useLiteMode,
            instrumentPropertySource="InstrumentDonor",
            instrumentSource=self.sampleData.donor,
            **kwargs,
        )

    def grouping(self, scheme: str = SCHEME, runNumber: str = RUN, useLiteMode: bool = False) -> GroceryListItem:
        return self.item("grouping", runNumber, useLiteMode, groupingScheme=scheme)

    def pixelMask(self, runNumber: str = RUN, useLiteMode: bool = False, timestamp: float = TIMESTAMP):
        return self.item("reduction_pixel_mask", runNumber, useLiteMode, timestamp=timestamp)

    def monitor(self, runNumber: str = RUN, useLiteMode: bool = False) -> GroceryListItem:
        return GroceryListItem(workspaceType="neutron", runNumber=runNumber, useLiteMode=useLiteMode)

    def fetch(self, item: GroceryListItem) -> str:
        [workspace] = self.service.fetchGroceryList([item])
        return workspace


def dataSource(workspace: str) -> str:
    """The provenance tag that SNAPRed writes to each fetched workspace that has logs."""
    return mtd[workspace].getRun().getLogData(f"{Config['metadata.tagPrefix']}dataSource").value


def fetchedWorkspaces() -> set:
    """The workspaces of these runs in the ADS, including hidden ones (grouping workspaces are hidden)."""
    with amend_config(**{"MantidOptions.InvisibleWorkspaces": "1"}):
        names = mtd.getObjectNames()
    return {name for name in names if RUN in name or OTHER_RUN in name or name == wng.liteDataMap().build()}


def clearFetchedWorkspaces():
    for ws in fetchedWorkspaces():
        if mtd.doesExist(ws):
            DeleteWorkspace(ws)


@pytest.fixture
def harness(sampleData, tmp_path, monkeypatch):
    clearFetchedWorkspaces()
    with Config_override("instrument.lite.map.file", LITE_GROUPING_MAP):
        yield Harness(sampleData, tmp_path, monkeypatch)
    clearFetchedWorkspaces()


class TestGrouping:
    @pytest.mark.parametrize("useLiteMode", [False, True])
    def test_fetch(self, harness, useLiteMode):
        workspace = harness.fetch(harness.grouping(useLiteMode=useLiteMode))
        assert workspace == groupingName(useLiteMode=useLiteMode)
        assert isinstance(mtd[workspace], GroupingWorkspace)
        assert mtd[workspace].getNumberHistograms() == harness.sampleData.pixelCount
        assert dataSource(workspace) == "groupingFile"
        assert fetchedWorkspaces() == {workspace}

    def test_lite_scheme(self, harness):
        # The `Lite` scheme is the lite-mode map, from `instrument.lite.map.file`, not from the grouping map.
        harness.groupingDefinitions.clear()
        workspace = harness.fetch(harness.grouping(LITE_SCHEME))
        assert workspace == wng.liteDataMap().build()
        assert isinstance(mtd[workspace], GroupingWorkspace)

    def test_schemes_are_separate(self, harness):
        assert harness.fetch(harness.grouping(SCHEME)) == groupingName(SCHEME)
        assert harness.fetch(harness.grouping(OTHER_SCHEME)) == groupingName(OTHER_SCHEME)
        assert fetchedWorkspaces() == {groupingName(SCHEME), groupingName(OTHER_SCHEME)}

    def test_resolutions_are_separate(self, harness):
        assert harness.fetch(harness.grouping(useLiteMode=False)) == groupingName(useLiteMode=False)
        assert harness.fetch(harness.grouping(useLiteMode=True)) == groupingName(useLiteMode=True)
        assert fetchedWorkspaces() == {groupingName(useLiteMode=False), groupingName(useLiteMode=True)}

    def test_cached(self, harness):
        path = harness.addGroupingDefinition(SCHEME)
        harness.fetch(harness.grouping())
        path.unlink()
        assert harness.fetch(harness.grouping()) == groupingName()

    def test_reused_for_another_run_of_the_same_state(self, harness):
        path = harness.addGroupingDefinition(SCHEME)
        harness.fetch(harness.grouping(runNumber=RUN))
        path.unlink()
        # The grouping that was loaded for the first run is used, under its name.
        assert harness.fetch(harness.grouping(runNumber=OTHER_RUN)) == groupingName(runNumber=RUN)
        assert not mtd.doesExist(groupingName(runNumber=OTHER_RUN))

    def test_deleted_grouping_is_reloaded_under_the_cached_name(self, harness):
        harness.fetch(harness.grouping(runNumber=RUN))
        DeleteWorkspace(groupingName(runNumber=RUN))
        # The name that was cached for the state is used, although it names the other run.
        assert harness.fetch(harness.grouping(runNumber=OTHER_RUN)) == groupingName(runNumber=RUN)
        assert isinstance(mtd[groupingName(runNumber=RUN)], GroupingWorkspace)
        assert not mtd.doesExist(groupingName(runNumber=OTHER_RUN))

    def test_resident_workspace_is_used(self, harness):
        # A workspace that is already in the ADS, under the grouping's name, is used without a load.
        LoadGroupingDefinition(
            GroupingFilename=GROUPING_DEFINITIONS[OTHER_SCHEME],
            InstrumentDonor=harness.sampleData.donor,
            OutputWorkspace=groupingName(SCHEME),
        )
        expected = mtd[groupingName(SCHEME)].extractY()
        harness.groupingDefinitions.clear()
        assert harness.fetch(harness.grouping(SCHEME)) == groupingName(SCHEME)
        assert (mtd[groupingName(SCHEME)].extractY() == expected).all()
        assert groupingName(SCHEME) in harness.service.getCachedWorkspaces()

    def test_cached_workspaces(self, harness):
        harness.fetch(harness.grouping())
        assert groupingName() in harness.service.getCachedWorkspaces()
        DeleteWorkspace(groupingName())
        harness.service.rebuildCache()
        assert groupingName() not in harness.service.getCachedWorkspaces()

    def test_no_definition(self, harness):
        harness.groupingDefinitions.clear()
        with pytest.raises(KeyError):
            harness.fetch(harness.grouping())
        assert fetchedWorkspaces() == set()


class TestReductionPixelMask:
    @pytest.mark.parametrize("useLiteMode", [False, True])
    def test_fetch(self, harness, useLiteMode):
        harness.addPixelMask(useLiteMode=useLiteMode)
        workspace = harness.fetch(harness.pixelMask(useLiteMode=useLiteMode))
        assert workspace == pixelMaskName()
        assert isinstance(mtd[workspace], MaskWorkspace)
        assert dataSource(workspace) == "reductionRecord"
        # Only the mask is loaded: the table in the same file is not.
        assert fetchedWorkspaces() == {workspace}

    def test_cached(self, harness):
        path = harness.addPixelMask()
        harness.fetch(harness.pixelMask())
        path.unlink()
        assert harness.fetch(harness.pixelMask()) == pixelMaskName()

    def test_timestamps_are_separate(self, harness):
        harness.addPixelMask(timestamp=TIMESTAMP)
        harness.addPixelMask(timestamp=OTHER_TIMESTAMP)
        assert harness.fetch(harness.pixelMask(timestamp=TIMESTAMP)) == pixelMaskName(timestamp=TIMESTAMP)
        assert harness.fetch(harness.pixelMask(timestamp=OTHER_TIMESTAMP)) == pixelMaskName(timestamp=OTHER_TIMESTAMP)
        assert fetchedWorkspaces() == {pixelMaskName(timestamp=TIMESTAMP), pixelMaskName(timestamp=OTHER_TIMESTAMP)}

    def test_not_a_cached_workspace(self, harness):
        # A pixel mask is reused while it is in the ADS, but it is not one of the service's cached workspaces.
        harness.addPixelMask()
        harness.fetch(harness.pixelMask())
        assert pixelMaskName() not in harness.service.getCachedWorkspaces()

    def test_no_file(self, harness):
        with pytest.raises(Exception):  # noqa: PT011, B017
            harness.fetch(harness.pixelMask())
        assert not mtd.doesExist(pixelMaskName())


class TestMonitor:
    @pytest.mark.parametrize("useLiteMode", [False, True])
    def test_fetch(self, harness, useLiteMode):
        # A monitor has no lite-mode form: a lite-mode item fetches the same, native, monitor.
        harness.addNeutronFile()
        workspace = harness.service.fetchMonitorWorkspace(harness.monitor(useLiteMode=useLiteMode))
        assert workspace == monitorName()
        assert mtd[workspace].getNumberHistograms() == 1
        assert list(mtd[workspace].readY(0)) == list(range(10))

    def test_reused_while_resident(self, harness):
        # A monitor that is in the ADS is reused; once it is deleted, it is loaded again.
        harness.addNeutronFile(counts=np.arange(10))
        harness.service.fetchMonitorWorkspace(harness.monitor())
        harness.addNeutronFile(counts=np.arange(10) * 2)
        workspace = harness.service.fetchMonitorWorkspace(harness.monitor())
        assert list(mtd[workspace].readY(0)) == list(range(10))
        DeleteWorkspace(workspace)
        workspace = harness.service.fetchMonitorWorkspace(harness.monitor())
        assert list(mtd[workspace].readY(0)) == list(range(0, 20, 2))

    def test_not_a_cached_workspace(self, harness):
        harness.addNeutronFile()
        harness.service.fetchMonitorWorkspace(harness.monitor())
        assert monitorName() not in harness.service.getCachedWorkspaces()

    def test_no_ipts_directory(self, harness):
        # `ReductionService` matches this message.
        with pytest.raises(RuntimeError, match=r"Cannot find IPTS directory for run '57482'"):
            harness.service.fetchMonitorWorkspace(harness.monitor())
        assert not mtd.doesExist(monitorName())
