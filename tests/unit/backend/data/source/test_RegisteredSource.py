import datetime

import pytest
from mantid.simpleapi import AddSampleLog, CloneWorkspace, CreateSingleValuedWorkspace, DeleteWorkspace, mtd

from snapred.backend.dao.ingredients.GroceryListItem import GroceryListItem, LiveDataArgs
from snapred.backend.data.source.DataSourceKind import DataSourceKind
from snapred.backend.data.source.NeutronDataRegistrations import NeutronDataRegistrations
from snapred.backend.data.source.RegisteredSource import RegisteredSource
from snapred.backend.data.source.Resolution import Resolution

RUN_NUMBER = "12345"


class Workspaces:
    """The ADS operations of `GroceryService`, on the real ADS."""

    def getCloneOfWorkspace(self, name, copy):
        CloneWorkspace(InputWorkspace=name, OutputWorkspace=copy)
        return copy

    def getWorkspaceForName(self, name):
        return mtd[name]


@pytest.fixture
def registered():
    name = mtd.unique_name(prefix="accumulation_")
    CreateSingleValuedWorkspace(OutputWorkspace=name)
    output = mtd.unique_name(prefix="output_")
    yield name, output
    for ws in (name, output):
        if mtd.doesExist(ws):
            DeleteWorkspace(ws)


def item(liveDataArgs=None) -> GroceryListItem:
    return GroceryListItem(workspaceType="neutron", runNumber=RUN_NUMBER, useLiteMode=False, liveDataArgs=liveDataArgs)


def test_kind():
    source = RegisteredSource(Workspaces())
    assert source.kind == DataSourceKind.REGISTERED
    assert source.provides == Resolution.NATIVE
    assert not source.cacheable


def test_not_registered():
    source = RegisteredSource(Workspaces())
    assert not source.available(item())
    assert not source.exclusiveFor(item())
    with pytest.raises(RuntimeError, match=r"no workspace is registered"):
        source.load(item(), "out")


def test_registered_is_exclusive(registered):
    name, _ = registered
    NeutronDataRegistrations().register(RUN_NUMBER, name)
    source = RegisteredSource(Workspaces())
    assert source.available(item())
    assert source.exclusiveFor(item())


def test_deleted_workspace_is_an_error(registered):
    name, _ = registered
    NeutronDataRegistrations().register(RUN_NUMBER, name)
    DeleteWorkspace(name)
    source = RegisteredSource(Workspaces())
    with pytest.raises(RuntimeError, match=r"no longer exists"):
        source.exclusiveFor(item())
    with pytest.raises(RuntimeError, match=r"no longer exists"):
        source.available(item())


def test_live_data_request_is_an_error(registered):
    name, _ = registered
    NeutronDataRegistrations().register(RUN_NUMBER, name)
    with pytest.raises(RuntimeError, match=r"Remove the registration first"):
        RegisteredSource(Workspaces()).exclusiveFor(item(LiveDataArgs(duration=datetime.timedelta(seconds=10))))


def test_load_clones_and_sets_the_run_number(registered):
    name, output = registered
    NeutronDataRegistrations().register(RUN_NUMBER, name)
    data = RegisteredSource(Workspaces()).load(item(), output)
    assert data.workspace == output
    assert data.loader == "CloneWorkspace"
    assert data.provenance == [DataSourceKind.REGISTERED]
    assert mtd[output].getRun().getLogData("run_number").value == RUN_NUMBER
    # the registered workspace is not changed
    assert not mtd[name].getRun().hasProperty("run_number")


def test_load_keeps_an_existing_run_number(registered):
    name, output = registered
    AddSampleLog(Workspace=name, LogName="run_number", LogText=RUN_NUMBER, LogType="String")
    NeutronDataRegistrations().register(RUN_NUMBER, name)
    RegisteredSource(Workspaces()).load(item(), output)
    assert mtd[output].getRun().getLogData("run_number").value == RUN_NUMBER
