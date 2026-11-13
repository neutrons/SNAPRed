from contextlib import ExitStack
from unittest import mock

import pytest
from mantid.simpleapi import CreateEmptyTableWorkspace, CreateSingleValuedWorkspace, DeleteWorkspace, mtd
from util.Config_helpers import Config_override

from snapred.backend.dao.ingredients.GroceryListItem import GroceryListItem
from snapred.backend.data.source.DataCache import ResidentDataCache, SingleEntryDataCache
from snapred.backend.data.source.DataResolver import DataResolver
from snapred.backend.data.source.DataSourceKind import DataSourceKind
from snapred.backend.data.source.DataSourceRegistry import DataSourceRegistry
from snapred.backend.data.source.NamedDataSource import NamedDataSource
from snapred.backend.data.source.Product import Product
from snapred.backend.data.source.Resolution import Resolution
from snapred.backend.data.source.WorkspaceOperations import WorkspaceOperations
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper
from snapred.meta.Config import Config

K = DataSourceKind
P = Product.DIFFCAL_MASK


class FakeSource(NamedDataSource):
    def __init__(self, kind, name, exclusive=False, available=True, table=False):
        super().__init__(provides=Resolution.NATIVE, product=P)
        self._kind, self._name, self._exclusive, self._available, self._table = (
            kind,
            name,
            exclusive,
            available,
            table,
        )
        self.loads = []

    @property
    def kind(self):
        return self._kind

    def exclusiveFor(self, item):  # noqa: ARG002
        return self._exclusive

    def available(self, item):  # noqa: ARG002
        return self._available

    def outputName(self, item):  # noqa: ARG002
        return self._name

    def load(self, item, outputWorkspace):  # noqa: ARG002
        self.loads.append(outputWorkspace)
        if self._table:
            CreateEmptyTableWorkspace(OutputWorkspace=outputWorkspace)
        else:
            CreateSingleValuedWorkspace(OutputWorkspace=outputWorkspace)
        return self._sourced(outputWorkspace, f"load:{self.kind}")


def item(useLiteMode=False) -> GroceryListItem:
    return GroceryListItem(
        workspaceType="diffcal_mask", runNumber="12345", useLiteMode=useLiteMode, state="s", diffCalVersion=1
    )


@pytest.fixture
def workspaces():
    created = []
    yield WorkspaceOperations(MantidSnapper(None, "Utensils")), created
    for name in created:
        if mtd.doesExist(name):
            DeleteWorkspace(name)


@pytest.fixture
def resolver(workspaces):
    """Make a resolver for the sources, with the order configured for the duration of the test."""
    ops, _ = workspaces
    with ExitStack() as stack:

        def make(sources, order, cache=None):
            stack.enter_context(Config_override(f"dataSources.{P}.native", order))
            registry = DataSourceRegistry(Resolution.NATIVE, sources, product=P)
            return DataResolver(registry, cache or ResidentDataCache(ops), ops)

        yield make


def dataSource(workspace):
    return mtd[workspace].getRun().getLogData(f"{Config['metadata.tagPrefix']}dataSource").value


def test_first_available_source_loads_and_tags(workspaces, resolver):
    record = FakeSource(K.CALIBRATION_RECORD, "ws_record")
    file = FakeSource(K.CALIBRATION_FILE, "ws_file", available=False)
    workspaces[1].extend(["ws_record", "ws_file"])
    r = resolver([file, record], ["calibrationFile", "calibrationRecord"])
    data = r.resolve(item())
    assert data.workspace == "ws_record"
    assert data.loader == "load:calibrationRecord"
    assert data.provenance == [K.CALIBRATION_RECORD]
    assert dataSource("ws_record") == "calibrationRecord"
    assert file.loads == []


def test_exclusive_source_wins(workspaces, resolver):
    record = FakeSource(K.CALIBRATION_RECORD, "ws_record")
    file = FakeSource(K.CALIBRATION_FILE, "ws_file", exclusive=True)
    workspaces[1].extend(["ws_record", "ws_file"])
    r = resolver([file, record], ["calibrationRecord", "calibrationFile"])
    assert r.resolve(item()).workspace == "ws_file"
    assert record.loads == []


def test_resident_workspace_is_reused_with_its_provenance(workspaces, resolver):
    record = FakeSource(K.CALIBRATION_RECORD, "ws_record")
    workspaces[1].append("ws_record")
    r = resolver([record], ["calibrationRecord"])
    r.resolve(item())
    data = r.resolve(item())
    assert data.loader == "cached"
    assert data.provenance == [K.CALIBRATION_RECORD]
    assert record.loads == ["ws_record"]


def test_table_has_no_tag(workspaces, resolver):
    record = FakeSource(K.CALIBRATION_RECORD, "ws_table", table=True)
    workspaces[1].append("ws_table")
    r = resolver([record], ["calibrationRecord"])
    assert r.resolve(item()).provenance == [K.CALIBRATION_RECORD]
    # a reused table has an unknown provenance
    assert r.resolve(item()).provenance == []


def test_no_such_data(resolver):
    record = FakeSource(K.CALIBRATION_RECORD, None)
    r = resolver([record], ["calibrationRecord"])
    assert r.resolve(item()) is None
    assert record.loads == []


def test_no_source_available(resolver):
    record = FakeSource(K.CALIBRATION_RECORD, "ws", available=False)
    r = resolver([record], ["calibrationRecord"])
    with pytest.raises(RuntimeError, match=r"No source of native diffcalMask data is available for run '12345'"):
        r.resolve(item())


def test_resolution_is_checked(resolver):
    r = resolver([FakeSource(K.CALIBRATION_RECORD, "ws")], ["calibrationRecord"])
    with pytest.raises(RuntimeError, match=r"resolver of native diffcalMask data was asked for lite mode: True"):
        r.resolve(item(useLiteMode=True))


def test_single_entry_cache(workspaces, resolver):
    ops, created = workspaces
    cache = SingleEntryDataCache(ops)
    first = FakeSource(K.CALIBRATION_RECORD, "ws_first")
    created.extend(["ws_first", "ws_second"])
    r = resolver([first], ["calibrationRecord"], cache=cache)
    r.resolve(item())
    assert cache.cachedWorkspaces() == ["ws_first"]
    first._name = "ws_second"
    r.resolve(item())
    assert not mtd.doesExist("ws_first")
    assert cache.cachedWorkspaces() == ["ws_second"]
    cache.clear()
    assert not mtd.doesExist("ws_second")
    assert cache.cachedWorkspaces() == []


def test_resident_cache_policy():
    ops = mock.Mock()
    ops.workspaceDoesExist.return_value = True
    cache = ResidentDataCache(ops)
    assert cache.contains("ws")
    cache.beforeLoad("ws")
    cache.record("ws")
    assert cache.cachedWorkspaces() == []
