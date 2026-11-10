"""
Behavior tests for the neutron-data fetch of `GroceryService`.

These tests exercise only the public fetch methods, and observe only:

* the returned dictionary;
* the workspaces in the ADS, and their logs (including the `dataSource` provenance tag);
* `getCachedWorkspaces`;
* the calls to the external boundaries: the live-data listener (via `FetchGroceriesRecipe`),
  and the lite-mode conversion (via `LiteDataService`).

They do not depend on any private attribute or method of `GroceryService`, so that its internal
structure can be changed without changing these tests.
"""

import datetime
import json
from pathlib import Path

import pytest
from mantid.simpleapi import (
    AddSampleLog,
    CloneWorkspace,
    CreateSampleWorkspace,
    DeleteWorkspace,
    LoadInstrument,
    SaveNexusProcessed,
    mtd,
)
from util.dao import DAOFactory
from util.instrument_helpers import addInstrumentLogs, getInstrumentLogDescriptors
from util.WhateversInTheFridge import WhateversInTheFridge

import snapred.backend.recipe.algorithm  # noqa: F401
from snapred.backend.dao.ingredients.GroceryListItem import GroceryListItem, LiveDataArgs
from snapred.backend.dao.RunMetadata import RunMetadata
from snapred.backend.data.GroceryService import GroceryService
from snapred.backend.error.LiveDataState import LiveDataState
from snapred.backend.error.RunStatus import RunStatus
from snapred.meta.Config import Config, Resource
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceNameGenerator as wng

RUN_NUMBER = "57482"
OTHER_RUN_NUMBER = "57483"
LIVE_LOADER = "LoadLiveDataInterval"


def _name(runNumber: str, useLiteMode: bool, auxiliary: str | None = None) -> str:
    builder = wng.run().runNumber(runNumber)
    if useLiteMode:
        builder.lite(wng.Lite.TRUE)
    if auxiliary is not None:
        builder.auxiliary(auxiliary)
    return builder.build()


def plainName(runNumber: str, useLiteMode: bool) -> str:
    return _name(runNumber, useLiteMode)


def rawName(runNumber: str, useLiteMode: bool) -> str:
    return _name(runNumber, useLiteMode, "Raw")


def copyName(runNumber: str, useLiteMode: bool, n: int) -> str:
    return _name(runNumber, useLiteMode, f"Copy{n}")


def runWorkspaces(runNumber: str = RUN_NUMBER) -> set:
    """The ADS-resident workspaces that belong to the run."""
    return {name for name in mtd.getObjectNames() if runNumber in name}


@pytest.fixture(scope="module")
def sampleData(tmp_path_factory):
    """
    A native-resolution and a "lite-mode" event workspace, on the fake SNAP instrument,
    and their files, at the paths that the data service will report.
    """
    root = tmp_path_factory.mktemp("neutronData")
    native = mtd.unique_hidden_name()
    CreateSampleWorkspace(OutputWorkspace=native, WorkspaceType="Event", XUnit="TOF", NumBanks=4, BankPixelWidth=2)
    LoadInstrument(
        Workspace=native,
        Filename=Resource.getPath("inputs/testInstrument/fakeSNAP_Definition.xml"),
        RewriteSpectraMap=True,
    )
    addInstrumentLogs(native, **getInstrumentLogDescriptors(DAOFactory.real_detector_state))

    # The workspace that the (mocked) live-data listener returns: the listener does not always set
    #   the 'run_number' log.
    live = mtd.unique_hidden_name()
    CloneWorkspace(InputWorkspace=native, OutputWorkspace=live)
    AddSampleLog(Workspace=live, LogName="sample_origin", LogText="live", LogType="String")

    AddSampleLog(Workspace=native, LogName="run_number", LogText=RUN_NUMBER, LogType="String")

    # The "lite-mode" file is distinguished from the native file only by a log value:
    #   these tests do not depend on its resolution.
    lite = mtd.unique_hidden_name()
    CloneWorkspace(InputWorkspace=native, OutputWorkspace=lite)
    AddSampleLog(Workspace=lite, LogName="sample_origin", LogText="liteFile", LogType="String")
    AddSampleLog(Workspace=native, LogName="sample_origin", LogText="nexusFile", LogType="String")

    nativePath = root / "native.nxs.h5"
    litePath = root / "lite.nxs.h5"
    SaveNexusProcessed(InputWorkspace=native, Filename=str(nativePath))
    SaveNexusProcessed(InputWorkspace=lite, Filename=str(litePath))

    yield {"nativePath": nativePath, "litePath": litePath, "live": live, "missingPath": root / "missing.nxs.h5"}

    for ws in (native, lite, live):
        if mtd.doesExist(ws):
            DeleteWorkspace(ws)


class Harness:
    """
    A `GroceryService`, with its external boundaries replaced:

    * which neutron-data files exist;
    * whether there is a live-data connection, and what the live-data listener returns;
    * the lite-mode conversion, which records whether it would export its result.
    """

    def __init__(self, sampleData, monkeypatch):
        self.sampleData = sampleData
        self.nativeOnDisk = True
        self.liteOnDisk = False
        self.liveConnection = False
        self.liveRunNumber: str | None = RUN_NUMBER
        self.liveRunStatus: str | None = None
        self.liveCalls = []
        self.exports = []

        fridge = WhateversInTheFridge()
        self.service = GroceryService(dataService=fridge)

        monkeypatch.setattr(fridge, "createNeutronFilePath", self._filePath)
        monkeypatch.setattr(fridge, "hasLiveDataConnection", lambda: self.liveConnection)
        instrumentState = DAOFactory.calibrationParameters().instrumentState
        monkeypatch.setattr(fridge, "generateInstrumentState", lambda _runNumber: instrumentState)

        realExecuteRecipe = self.service.grocer.executeRecipe

        def executeRecipe(filename: str = "", workspace: str = "", loader: str = "", **kwargs):
            if loader == LIVE_LOADER:
                return self._loadLive(workspace, kwargs)
            return realExecuteRecipe(filename, workspace, loader, **kwargs)

        monkeypatch.setattr(self.service.grocer, "executeRecipe", executeRecipe)

        harness = self

        class FakeLiteDataService:
            def createLiteData(self, inputWorkspace, outputWorkspace, instrumentDefinition=None, export=True):  # noqa: ARG002
                harness.exports.append((outputWorkspace, export))
                AddSampleLog(Workspace=outputWorkspace, LogName="converted_to_lite", LogText="1", LogType="String")
                return outputWorkspace, 0.0

        monkeypatch.setattr("snapred.backend.service.LiteDataService.LiteDataService", FakeLiteDataService)

    def _filePath(self, runNumber: str, useLiteMode: bool) -> Path:  # noqa: ARG002
        if useLiteMode:
            return self.sampleData["litePath"] if self.liteOnDisk else self.sampleData["missingPath"]
        return self.sampleData["nativePath"] if self.nativeOnDisk else self.sampleData["missingPath"]

    def _loadLive(self, workspace: str, kwargs):
        self.liveCalls.append((workspace, kwargs))
        CloneWorkspace(InputWorkspace=self.sampleData["live"], OutputWorkspace=workspace)
        if self.liveRunNumber is not None:
            mtd[workspace].mutableRun().addProperty("run_number", self.liveRunNumber, True)
        data = {"result": True, "loader": LIVE_LOADER, "workspace": workspace}
        if self.liveRunStatus is not None:
            data["runStatus"] = self.liveRunStatus
        return data

    @staticmethod
    def item(useLiteMode: bool, liveDataArgs: LiveDataArgs | None = None) -> GroceryListItem:
        return GroceryListItem(
            workspaceType="neutron",
            runNumber=RUN_NUMBER,
            useLiteMode=useLiteMode,
            loader="",
            state="stateId",
            liveDataArgs=liveDataArgs,
        )


def origin(workspace: str) -> str:
    return mtd[workspace].getRun().getLogData("sample_origin").value


def dataSource(workspace: str) -> str:
    """The provenance tag that SNAPRed writes to every fetched neutron-data workspace."""
    return mtd[workspace].getRun().getLogData(f"{Config['metadata.tagPrefix']}dataSource").value


def assertDataSource(result, expected: str):
    assert result["dataSource"] == expected
    assert dataSource(result["workspace"]) == expected


def convertedToLite(workspace: str) -> bool:
    return mtd[workspace].getRun().hasProperty("converted_to_lite")


@pytest.fixture
def harness(sampleData, monkeypatch):
    for ws in runWorkspaces() | runWorkspaces(OTHER_RUN_NUMBER):
        DeleteWorkspace(ws)
    assert Config["mantid.workspace.normalizeByBeamMonitor"] is False
    yield Harness(sampleData, monkeypatch)
    for ws in runWorkspaces() | runWorkspaces(OTHER_RUN_NUMBER):
        DeleteWorkspace(ws)


LIVE_ARGS = LiveDataArgs(duration=datetime.timedelta(seconds=30))


class TestFileData:
    def test_native_single_use(self, harness):
        result = harness.service.fetchNeutronDataSingleUse(harness.item(False))
        assert result["result"]
        assert result["loader"] == "LoadNexusProcessed"
        assert result["workspace"] == plainName(RUN_NUMBER, False)
        assert runWorkspaces() == {plainName(RUN_NUMBER, False)}
        assert origin(result["workspace"]) == "nexusFile"
        assertDataSource(result, "nexusFile")
        assert harness.service.getCachedWorkspaces() == []
        assert harness.exports == []

    def test_native_cached(self, harness):
        first = harness.service.fetchNeutronDataCached(harness.item(False))
        assert first["loader"] == "LoadNexusProcessed"
        assert first["workspace"] == copyName(RUN_NUMBER, False, 1)

        second = harness.service.fetchNeutronDataCached(harness.item(False))
        assert second["result"]
        assert second["loader"] == "cached"
        assert second["workspace"] == copyName(RUN_NUMBER, False, 2)

        # A cache hit also leaves a copy under the plain name.
        assert runWorkspaces() == {
            plainName(RUN_NUMBER, False),
            rawName(RUN_NUMBER, False),
            copyName(RUN_NUMBER, False, 1),
            copyName(RUN_NUMBER, False, 2),
        }
        assert harness.service.getCachedWorkspaces() == [rawName(RUN_NUMBER, False)]
        assert origin(second["workspace"]) == "nexusFile"
        # a copy of cached data keeps the provenance of the original
        assertDataSource(first, "nexusFile")
        assertDataSource(second, "nexusFile")

    def test_native_single_use_after_cached_uses_cache(self, harness):
        harness.service.fetchNeutronDataCached(harness.item(False))
        harness.nativeOnDisk = False
        result = harness.service.fetchNeutronDataSingleUse(harness.item(False))
        assert result["loader"] == "cached"
        assert result["workspace"] == plainName(RUN_NUMBER, False)
        assert origin(result["workspace"]) == "nexusFile"

    def test_cache_entry_is_dropped_when_raw_workspace_is_deleted(self, harness):
        harness.service.fetchNeutronDataCached(harness.item(False))
        DeleteWorkspace(rawName(RUN_NUMBER, False))
        result = harness.service.fetchNeutronDataCached(harness.item(False))
        assert result["loader"] == "LoadNexusProcessed"
        assert result["workspace"] == copyName(RUN_NUMBER, False, 1)

    def test_lite_file_cached(self, harness):
        harness.liteOnDisk = True
        result = harness.service.fetchNeutronDataCached(harness.item(True))
        assert result["loader"] == "LoadNexusProcessed"
        assert result["workspace"] == copyName(RUN_NUMBER, True, 1)
        assert origin(result["workspace"]) == "liteFile"
        assertDataSource(result, "liteFile")
        assert not convertedToLite(result["workspace"])
        assert harness.exports == []
        assert runWorkspaces() == {rawName(RUN_NUMBER, True), copyName(RUN_NUMBER, True, 1)}
        assert harness.service.getCachedWorkspaces() == [rawName(RUN_NUMBER, True)]

    def test_lite_single_use_from_lite_file(self, harness):
        harness.liteOnDisk = True
        result = harness.service.fetchNeutronDataSingleUse(harness.item(True))
        assert result["workspace"] == plainName(RUN_NUMBER, True)
        assert origin(result["workspace"]) == "liteFile"
        assert harness.exports == []

    def test_lite_cached_converted_from_native_file(self, harness):
        first = harness.service.fetchNeutronDataCached(harness.item(True))
        assert first["result"]
        assert first["loader"] == "LoadNexusProcessed"
        assert first["workspace"] == copyName(RUN_NUMBER, True, 1)
        assert origin(first["workspace"]) == "nexusFile"
        assert convertedToLite(first["workspace"])
        assertDataSource(first, "nexusFile>liteConversion")
        assert dataSource(rawName(RUN_NUMBER, False)) == "nexusFile"
        # converted from file data: it is exported
        assert harness.exports == [(plainName(RUN_NUMBER, True), True)]
        assert runWorkspaces() == {
            rawName(RUN_NUMBER, False),
            rawName(RUN_NUMBER, True),
            copyName(RUN_NUMBER, True, 1),
        }
        assert sorted(harness.service.getCachedWorkspaces()) == sorted(
            [rawName(RUN_NUMBER, False), rawName(RUN_NUMBER, True)]
        )

        second = harness.service.fetchNeutronDataCached(harness.item(True))
        assert second["loader"] == "cached"
        assert second["workspace"] == copyName(RUN_NUMBER, True, 2)
        assert len(harness.exports) == 1

    def test_lite_cached_converted_from_cached_native(self, harness):
        harness.service.fetchNeutronDataCached(harness.item(False))
        harness.nativeOnDisk = False
        result = harness.service.fetchNeutronDataCached(harness.item(True))
        assert result["loader"] == "cached"
        assert result["workspace"] == copyName(RUN_NUMBER, True, 1)
        assert convertedToLite(result["workspace"])
        assertDataSource(result, "nexusFile>liteConversion")
        assert harness.exports == [(plainName(RUN_NUMBER, True), True)]

    def test_lite_single_use_converted_from_native_file(self, harness):
        result = harness.service.fetchNeutronDataSingleUse(harness.item(True))
        assert result["loader"] == "LoadNexusProcessed"
        assert result["workspace"] == plainName(RUN_NUMBER, True)
        assert convertedToLite(result["workspace"])
        assert harness.exports == [(plainName(RUN_NUMBER, True), True)]
        # the native input of the conversion is left in the ADS
        assert runWorkspaces() == {plainName(RUN_NUMBER, False), plainName(RUN_NUMBER, True)}
        assert harness.service.getCachedWorkspaces() == []

    @pytest.mark.parametrize("useLiteMode", [False, True])
    def test_no_data_and_no_live_connection(self, harness, useLiteMode):
        harness.nativeOnDisk = False
        with pytest.raises(RuntimeError, match=r"is not present on disk, and no live-data connection is available"):
            harness.service.fetchNeutronDataCached(harness.item(useLiteMode))
        assert runWorkspaces() == set()
        assert harness.service.getCachedWorkspaces() == []


class TestLiveDataFallback:
    @pytest.fixture(autouse=True)
    def _noFile(self, harness):
        harness.nativeOnDisk = False
        harness.liveConnection = True

    def test_native_single_use(self, harness):
        result = harness.service.fetchNeutronDataSingleUse(harness.item(False))
        assert result["result"]
        assert result["loader"] == LIVE_LOADER
        assert result["workspace"] == plainName(RUN_NUMBER, False)
        assert origin(result["workspace"]) == "live"
        assertDataSource(result, "live")
        assert len(harness.liveCalls) == 1
        workspace, _ = harness.liveCalls[0]
        assert workspace == plainName(RUN_NUMBER, False)

    def test_lite_cached_is_not_exported(self, harness):
        result = harness.service.fetchNeutronDataCached(harness.item(True))
        assert result["workspace"] == copyName(RUN_NUMBER, True, 1)
        assert origin(result["workspace"]) == "live"
        assert convertedToLite(result["workspace"])
        assertDataSource(result, "live>liteConversion")
        assert harness.exports == [(plainName(RUN_NUMBER, True), False)]

    def test_native_cached_repeat(self, harness):
        first = harness.service.fetchNeutronDataCached(harness.item(False))
        assert first["loader"] == LIVE_LOADER
        assert first["workspace"] == copyName(RUN_NUMBER, False, 1)
        # Live data is entered into the cache, but under its plain name, not as the raw workspace:
        #   so it is not reused.
        assert runWorkspaces() == {plainName(RUN_NUMBER, False), copyName(RUN_NUMBER, False, 1)}

        second = harness.service.fetchNeutronDataCached(harness.item(False))
        assert second["loader"] == LIVE_LOADER
        assert second["workspace"] == copyName(RUN_NUMBER, False, 1)
        assert len(harness.liveCalls) == 2

    def test_not_the_live_run(self, harness):
        harness.liveRunNumber = OTHER_RUN_NUMBER
        with pytest.raises(RuntimeError, match=r"is not present on disk, nor is it the live-data run"):
            harness.service.fetchNeutronDataCached(harness.item(False))
        assert runWorkspaces() == set()
        assert runWorkspaces(OTHER_RUN_NUMBER) == set()

    def test_live_run_number_not_set(self, harness):
        harness.liveRunNumber = None
        with pytest.raises(RuntimeError, match=r"is not present on disk, nor is it the live-data run"):
            harness.service.fetchNeutronDataCached(harness.item(False))
        assert runWorkspaces() == set()

    def test_live_run_not_running(self, harness):
        harness.liveRunStatus = RunStatus.PAUSED.value
        with pytest.raises(RuntimeError, match=r"there is a problem with the live-data run"):
            harness.service.fetchNeutronDataCached(harness.item(False))
        assert runWorkspaces() == set()


class TestLiveDataRequested:
    @pytest.fixture(autouse=True)
    def _liveConnection(self, harness):
        harness.liveConnection = True

    def test_file_is_not_used(self, harness):
        harness.liteOnDisk = True
        result = harness.service.fetchNeutronDataCached(harness.item(True, LIVE_ARGS))
        assert result["workspace"] == copyName(RUN_NUMBER, True, 1)
        assert origin(result["workspace"]) == "live"
        assertDataSource(result, "live>liteConversion")
        assert harness.exports == [(plainName(RUN_NUMBER, True), False)]
        assert len(harness.liveCalls) == 1

    def test_start_time(self, harness):
        before = datetime.datetime.utcnow()
        harness.service.fetchNeutronDataSingleUse(harness.item(False, LIVE_ARGS))
        after = datetime.datetime.utcnow()
        _, kwargs = harness.liveCalls[0]
        loaderArgs = json.loads(kwargs["loaderArgs"])
        startTime = datetime.datetime.fromisoformat(loaderArgs["StartTime"])
        assert before - LIVE_ARGS.duration <= startTime <= after - LIVE_ARGS.duration
        assert loaderArgs["PreserveEvents"] is True

    def test_from_start_of_run(self, harness):
        harness.service.fetchNeutronDataSingleUse(harness.item(False, LiveDataArgs(duration=datetime.timedelta(0))))
        _, kwargs = harness.liveCalls[0]
        assert json.loads(kwargs["loaderArgs"])["StartTime"] == RunMetadata.FROM_START_ISO8601

    def test_run_number_not_set_is_filled_in(self, harness):
        harness.liveRunNumber = None
        result = harness.service.fetchNeutronDataSingleUse(harness.item(False, LIVE_ARGS))
        assert mtd[result["workspace"]].getRun().getLogData("run_number").value == RUN_NUMBER

    def test_run_transition(self, harness):
        harness.liveRunNumber = OTHER_RUN_NUMBER
        with pytest.raises(LiveDataState):
            harness.service.fetchNeutronDataCached(harness.item(False, LIVE_ARGS))
        assert runWorkspaces() == set()

    @pytest.mark.parametrize("status", [RunStatus.PAUSED, RunStatus.STOPPED, RunStatus.ERROR])
    def test_run_status(self, harness, status):
        harness.liveRunStatus = status.value
        with pytest.raises(LiveDataState):
            harness.service.fetchNeutronDataCached(harness.item(False, LIVE_ARGS))
        assert runWorkspaces() == set()

    def test_no_live_connection(self, harness):
        harness.liveConnection = False
        with pytest.raises(RuntimeError, match=r"no live-data connection is available"):
            harness.service.fetchNeutronDataCached(harness.item(False, LIVE_ARGS))

    def test_clearLiveDataCache(self, harness):
        harness.service.fetchNeutronDataCached(harness.item(True, LIVE_ARGS))
        before = runWorkspaces()
        assert before == {plainName(RUN_NUMBER, False), rawName(RUN_NUMBER, True), copyName(RUN_NUMBER, True, 1)}
        assert sorted(harness.service.getCachedWorkspaces()) == sorted(
            [rawName(RUN_NUMBER, False), rawName(RUN_NUMBER, True)]
        )

        harness.service.clearLiveDataCache()

        # Only the native live-data entry is removed from the cache.  Its raw workspace never existed,
        #   so no workspace is deleted.
        assert runWorkspaces() == before
        assert harness.service.getCachedWorkspaces() == [rawName(RUN_NUMBER, True)]
