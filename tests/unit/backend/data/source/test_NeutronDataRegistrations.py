import pytest
from mantid.simpleapi import AddSampleLog, CreateSingleValuedWorkspace, DeleteWorkspace, mtd

from snapred.backend.data.source.NeutronDataRegistrations import NeutronDataRegistrations
from snapred.backend.error.LiveDataState import LiveDataState

RUN_NUMBER = "12345"
OTHER_RUN_NUMBER = "12346"


@pytest.fixture
def workspace():
    name = mtd.unique_name(prefix="accumulation_")
    CreateSingleValuedWorkspace(OutputWorkspace=name)
    yield name
    if mtd.doesExist(name):
        DeleteWorkspace(name)


def setRunNumber(workspace, runNumber):
    AddSampleLog(Workspace=workspace, LogName="run_number", LogText=runNumber, LogType="String")


def test_is_a_singleton():
    assert NeutronDataRegistrations() is NeutronDataRegistrations()


def test_register_and_lookup(workspace):
    registrations = NeutronDataRegistrations()
    assert registrations.lookup(RUN_NUMBER) is None
    registrations.register(RUN_NUMBER, workspace)
    assert registrations.lookup(RUN_NUMBER) == workspace
    assert registrations.lookup(int(RUN_NUMBER)) == workspace
    assert registrations.registeredWorkspaces() == [workspace]


def test_register_again_is_harmless(workspace):
    registrations = NeutronDataRegistrations()
    registrations.register(RUN_NUMBER, workspace)
    registrations.register(RUN_NUMBER, workspace)
    assert registrations.lookup(RUN_NUMBER) == workspace
    assert registrations.registeredWorkspaces() == [workspace]


def test_register_with_matching_run_number(workspace):
    setRunNumber(workspace, RUN_NUMBER)
    NeutronDataRegistrations().register(RUN_NUMBER, workspace)
    assert NeutronDataRegistrations().lookup(RUN_NUMBER) == workspace


def test_register_with_other_run_number(workspace):
    setRunNumber(workspace, OTHER_RUN_NUMBER)
    with pytest.raises(ValueError, match=r"its 'run_number' log is '12346'"):
        NeutronDataRegistrations().register(RUN_NUMBER, workspace)
    assert NeutronDataRegistrations().lookup(RUN_NUMBER) is None


def test_register_missing_workspace():
    with pytest.raises(ValueError, match=r"it does not exist"):
        NeutronDataRegistrations().register(RUN_NUMBER, "not_a_workspace")


def test_register_workspace_for_a_new_run(workspace):
    registrations = NeutronDataRegistrations()
    registrations.register(RUN_NUMBER, workspace)
    registrations.register(OTHER_RUN_NUMBER, workspace)
    assert registrations.lookup(RUN_NUMBER) is None
    assert registrations.lookup(OTHER_RUN_NUMBER) == workspace


def test_unregister(workspace):
    registrations = NeutronDataRegistrations()
    registrations.register(RUN_NUMBER, workspace)
    registrations.unregister(RUN_NUMBER)
    assert registrations.lookup(RUN_NUMBER) is None
    assert mtd.doesExist(workspace)
    # unregistering a run that is not registered is harmless
    registrations.unregister(RUN_NUMBER)


def test_lookup_deleted_workspace(workspace):
    # The registration is kept: it was not removed, so a missing workspace is an error.
    registrations = NeutronDataRegistrations()
    registrations.register(RUN_NUMBER, workspace)
    DeleteWorkspace(workspace)
    for _ in range(2):
        with pytest.raises(RuntimeError, match=r"no longer exists.*or remove the registration"):
            registrations.lookup(RUN_NUMBER)
    assert registrations.isRegistered(RUN_NUMBER)
    assert registrations.registeredWorkspaces() == [workspace]
    registrations.unregister(RUN_NUMBER)
    assert registrations.lookup(RUN_NUMBER) is None


def test_lookup_after_run_transition(workspace):
    registrations = NeutronDataRegistrations()
    registrations.register(RUN_NUMBER, workspace)
    setRunNumber(workspace, OTHER_RUN_NUMBER)
    with pytest.raises(LiveDataState) as e:
        registrations.lookup(RUN_NUMBER)
    assert e.value.transition == LiveDataState.Type.RUN_GAP
    # The registration is kept.
    assert registrations.isRegistered(RUN_NUMBER)
    with pytest.raises(LiveDataState):
        registrations.lookup(RUN_NUMBER)
    assert mtd.doesExist(workspace)


def test_isRegistered(workspace):
    registrations = NeutronDataRegistrations()
    assert not registrations.isRegistered(RUN_NUMBER)
    registrations.register(RUN_NUMBER, workspace)
    assert registrations.isRegistered(RUN_NUMBER)
    assert registrations.isRegistered(int(RUN_NUMBER))
