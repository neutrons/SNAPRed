from contextlib import ExitStack
from unittest import mock

import pytest
from util.Config_helpers import Config_override

from snapred.backend.dao.ingredients.GroceryListItem import GroceryListItem
from snapred.backend.dao.WorkspaceMetadata import UNSET, WorkspaceMetadata
from snapred.backend.data.source.DataSource import DataSource
from snapred.backend.data.source.DataSourceKind import DataSourceKind
from snapred.backend.data.source.NeutronDataCache import NeutronDataCache
from snapred.backend.data.source.NeutronDataResolver import NeutronDataResolver
from snapred.backend.data.source.NeutronWorkspaceNames import NeutronWorkspaceNames as names
from snapred.backend.data.source.Resolution import Resolution

ThisModule = "snapred.backend.data.source.NeutronDataResolver."
RUN_NUMBER = "12345"
K = DataSourceKind


class FakeSource(DataSource):
    def __init__(self, kind, provides, requires=None, available=True, cacheable=True):
        super().__init__(provides=provides, requires=requires)
        self._kind = kind
        self._available = available
        self.cacheable = cacheable
        self.loads = []

    @property
    def kind(self):
        return self._kind

    def available(self, item):  # noqa: ARG002
        return self._available

    def load(self, item, outputWorkspace, inputData=None):  # noqa: ARG002
        self.loads.append((outputWorkspace, inputData))
        return self._sourced(outputWorkspace, f"load:{self.kind}", inputData)


class Harness:
    def __init__(self, sources, native=("nexusFile", "live"), lite=("liteFile", "liteConversion")):
        self.ads = {}
        self.tags = {}
        self.workspaces = mock.Mock()
        self.workspaces.workspaceDoesExist.side_effect = lambda name: name in self.ads

        # As in the ADS, a clone or rename keeps the workspace's logs: here, its provenance tag.
        def clone(name, copy):
            self.ads[copy] = self.ads[name]
            if name in self.tags:
                self.tags[copy] = self.tags[name]
            return copy

        def rename(old, new):
            self.ads[new] = self.ads.pop(old)
            if old in self.tags:
                self.tags[new] = self.tags.pop(old)
            return new

        self.workspaces.getCloneOfWorkspace.side_effect = clone
        self.workspaces.renameWorkspace.side_effect = rename

        self.cache = NeutronDataCache(self.workspaces)
        self.sources = {source.kind: source for source in sources}
        for source in sources:
            # Loading a workspace puts it into the fake ADS, with its provenance tag.
            original = source.load

            def load(item, outputWorkspace, inputData=None, original=original):
                data = original(item, outputWorkspace, inputData)
                self.ads[outputWorkspace] = None
                return data

            source.load = load
        self.resolver = NeutronDataResolver(sources, self.cache, self.workspaces)

        self._stack = ExitStack()
        self._stack.enter_context(Config_override("neutronData.sources.native", list(native)))
        self._stack.enter_context(Config_override("neutronData.sources.lite", list(lite)))
        mockMetadataService = self._stack.enter_context(mock.patch(ThisModule + "WorkspaceMetadataService"))
        self.metadata = mockMetadataService.return_value
        self.metadata.writeWorkspaceMetadata.side_effect = lambda ws, md: self.tags.__setitem__(ws, md.dataSource)
        self.metadata.readMetadataTag.side_effect = lambda ws, _name: self.tags.get(ws, UNSET)

    def close(self):
        self._stack.close()


def standardSources(fileAvailable=True, liveAvailable=True, liteFileAvailable=False):
    return [
        FakeSource(K.NEXUS_FILE, Resolution.NATIVE, available=fileAvailable),
        FakeSource(K.LIVE, Resolution.NATIVE, available=liveAvailable, cacheable=False),
        FakeSource(K.LITE_FILE, Resolution.LITE, available=liteFileAvailable),
        FakeSource(K.LITE_CONVERSION, Resolution.LITE, requires=Resolution.NATIVE),
    ]


@pytest.fixture
def harness(request):
    h = Harness(**getattr(request, "param", {"sources": standardSources()}))
    yield h
    h.close()


def item(useLiteMode=False) -> GroceryListItem:
    return GroceryListItem(workspaceType="neutron", runNumber=RUN_NUMBER, useLiteMode=useLiteMode)


def test_first_available_source_in_order(harness):
    data = harness.resolver.resolve(item(), Resolution.NATIVE)
    assert data.provenance == [K.NEXUS_FILE]
    assert data.workspace == names.plain(RUN_NUMBER, False)
    assert harness.sources[K.LIVE].loads == []
    harness.metadata.writeWorkspaceMetadata.assert_called_once_with(
        data.workspace, WorkspaceMetadata(dataSource="nexusFile")
    )


@pytest.mark.parametrize("harness", [{"sources": standardSources(fileAvailable=False)}], indirect=True)
def test_unavailable_source_is_skipped(harness):
    data = harness.resolver.resolve(item(), Resolution.NATIVE)
    assert data.provenance == [K.LIVE]


@pytest.mark.parametrize("harness", [{"sources": standardSources(), "native": ("live", "nexusFile")}], indirect=True)
def test_order_is_configured(harness):
    assert harness.resolver.resolve(item(), Resolution.NATIVE).provenance == [K.LIVE]


@pytest.mark.parametrize(
    "harness", [{"sources": standardSources(fileAvailable=False, liveAvailable=False)}], indirect=True
)
def test_no_source_available(harness):
    with pytest.raises(RuntimeError, match=r"is not present on disk, and no live-data connection is available"):
        harness.resolver.resolve(item(), Resolution.NATIVE)


@pytest.mark.parametrize(
    "harness", [{"sources": standardSources(), "native": ("nexusFile", "liteFile")}], indirect=True
)
def test_misconfigured_order(harness):
    with pytest.raises(RuntimeError, match=r"lists 'liteFile', which provides lite data"):
        harness.resolver.resolve(item(), Resolution.NATIVE)


def test_required_input_is_resolved_first(harness):
    data = harness.resolver.resolve(item(True), Resolution.LITE)
    assert data.provenance == [K.NEXUS_FILE, K.LITE_CONVERSION]
    assert data.workspace == names.plain(RUN_NUMBER, True)
    [(output, inputData)] = harness.sources[K.LITE_CONVERSION].loads
    assert output == names.plain(RUN_NUMBER, True)
    assert inputData.workspace == names.plain(RUN_NUMBER, False)
    assert harness.tags == {
        names.plain(RUN_NUMBER, False): "nexusFile",
        names.plain(RUN_NUMBER, True): "nexusFile>liteConversion",
    }


@pytest.mark.parametrize("harness", [{"sources": standardSources(liteFileAvailable=True)}], indirect=True)
def test_lite_file_before_conversion(harness):
    assert harness.resolver.resolve(item(True), Resolution.LITE).provenance == [K.LITE_FILE]
    assert harness.sources[K.NEXUS_FILE].loads == []


@pytest.mark.parametrize("harness", [{"sources": standardSources(fileAvailable=False)}], indirect=True)
def test_uncacheable_source_is_recorded(harness):
    harness.resolver.resolve(item(), Resolution.NATIVE)
    assert harness.cache.uncacheableKeys == [(RUN_NUMBER, False)]
    assert harness.cache.loadedRuns == {(RUN_NUMBER, False): 0}


def test_cache_hit_copies_the_raw_workspace(harness):
    raw = names.raw(RUN_NUMBER, False)
    harness.ads[raw] = None
    harness.tags[raw] = "nexusFile"
    data = harness.resolver.resolve(item(), Resolution.NATIVE)
    assert data.loader == "cached"
    assert data.provenance == [K.NEXUS_FILE]
    harness.workspaces.getCloneOfWorkspace.assert_called_once_with(raw, names.plain(RUN_NUMBER, False))
    assert harness.sources[K.NEXUS_FILE].loads == []


def test_cache_hit_without_provenance(harness):
    harness.ads[names.raw(RUN_NUMBER, False)] = None
    assert harness.resolver.resolve(item(), Resolution.NATIVE).provenance == []


def test_fetchSingleUse(harness):
    result = harness.resolver.fetchSingleUse(item())
    assert result == {
        "result": True,
        "loader": "load:nexusFile",
        "workspace": names.plain(RUN_NUMBER, False),
        "dataSource": "nexusFile",
    }
    assert harness.cache.loadedRuns == {}


def test_fetchCached(harness):
    first = harness.resolver.fetchCached(item())
    assert first["workspace"] == names.copy(RUN_NUMBER, False, 1)
    assert first["dataSource"] == "nexusFile"
    assert names.raw(RUN_NUMBER, False) in harness.ads
    assert names.plain(RUN_NUMBER, False) not in harness.ads
    assert harness.cache.loadedRuns == {(RUN_NUMBER, False): 1}

    second = harness.resolver.fetchCached(item())
    assert second["loader"] == "cached"
    assert second["workspace"] == names.copy(RUN_NUMBER, False, 2)
    assert second["dataSource"] == "nexusFile"
    assert harness.cache.loadedRuns == {(RUN_NUMBER, False): 2}


def test_fetchCached_caches_the_converted_input(harness):
    result = harness.resolver.fetchCached(item(True))
    assert result["workspace"] == names.copy(RUN_NUMBER, True, 1)
    assert harness.cache.loadedRuns == {(RUN_NUMBER, True): 1, (RUN_NUMBER, False): 0}
    assert names.raw(RUN_NUMBER, False) in harness.ads
    assert names.raw(RUN_NUMBER, True) in harness.ads


@pytest.mark.parametrize("harness", [{"sources": standardSources(fileAvailable=False)}], indirect=True)
def test_fetchCached_uncacheable_is_not_reused(harness):
    first = harness.resolver.fetchCached(item())
    assert first["workspace"] == names.copy(RUN_NUMBER, False, 1)
    # not renamed to the raw workspace
    assert names.raw(RUN_NUMBER, False) not in harness.ads
    second = harness.resolver.fetchCached(item())
    assert second["loader"] == "load:live"
    assert second["workspace"] == names.copy(RUN_NUMBER, False, 1)
    assert len(harness.sources[K.LIVE].loads) == 2
