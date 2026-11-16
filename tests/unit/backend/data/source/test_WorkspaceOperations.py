import pytest
from mantid.simpleapi import CreateSingleValuedWorkspace, DeleteWorkspace, mtd

from snapred.backend.data.source.WorkspaceOperations import WorkspaceOperations
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper


@pytest.fixture
def workspaces():
    created = []

    def name(prefix):
        n = mtd.unique_name(prefix=prefix)
        created.append(n)
        return n

    yield WorkspaceOperations(MantidSnapper(None, "Utensils")), name
    for n in created:
        if mtd.doesExist(n):
            DeleteWorkspace(n)


def test_exists_and_get(workspaces):
    ops, name = workspaces
    ws = name("ws_")
    assert not ops.workspaceDoesExist(ws)
    assert ops.getWorkspaceForName(ws) is None
    CreateSingleValuedWorkspace(OutputWorkspace=ws, DataValue=3.0)
    assert ops.workspaceDoesExist(ws)
    assert ops.getWorkspaceForName(ws).readY(0)[0] == 3.0


def test_clone(workspaces):
    ops, name = workspaces
    ws, copy = name("ws_"), name("copy_")
    CreateSingleValuedWorkspace(OutputWorkspace=ws, DataValue=3.0)
    assert ops.getCloneOfWorkspace(ws, copy) == copy
    assert mtd.doesExist(ws)
    assert mtd[copy].readY(0)[0] == 3.0


def test_rename(workspaces):
    ops, name = workspaces
    ws, new = name("ws_"), name("new_")
    CreateSingleValuedWorkspace(OutputWorkspace=ws)
    assert ops.renameWorkspace(ws, new) == new
    assert not mtd.doesExist(ws)
    assert mtd.doesExist(new)
    # renaming to the same name does nothing
    assert ops.renameWorkspace(new, new) == new
    assert mtd.doesExist(new)


def test_delete(workspaces):
    ops, name = workspaces
    ws = name("ws_")
    CreateSingleValuedWorkspace(OutputWorkspace=ws)
    ops.deleteWorkspaceUnconditional(ws)
    assert not mtd.doesExist(ws)
    # deleting a workspace that does not exist is harmless
    ops.deleteWorkspaceUnconditional(ws)
