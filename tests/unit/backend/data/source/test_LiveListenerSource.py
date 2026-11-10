import datetime
import json
from unittest import mock

import pytest

from snapred.backend.dao.ingredients.GroceryListItem import GroceryListItem, LiveDataArgs
from snapred.backend.dao.RunMetadata import RunMetadata
from snapred.backend.data.source.DataSourceKind import DataSourceKind
from snapred.backend.data.source.LiveListenerSource import LiveListenerSource
from snapred.backend.data.source.Resolution import Resolution
from snapred.backend.error.LiveDataState import LiveDataState
from snapred.backend.error.RunStatus import RunStatus
from snapred.meta.Config import Config

RUN_NUMBER = "12345"
LIVE_ARGS = LiveDataArgs(duration=datetime.timedelta(seconds=30))


def item(liveDataArgs=None) -> GroceryListItem:
    return GroceryListItem(workspaceType="neutron", runNumber=RUN_NUMBER, useLiteMode=False, liveDataArgs=liveDataArgs)


class Harness:
    def __init__(self, liveRunNumber=RUN_NUMBER, runStatus=None, connected=True):
        self.dataService = mock.Mock()
        self.dataService.hasLiveDataConnection.return_value = connected

        self.run = mock.Mock()
        self.run.hasProperty.side_effect = lambda name: name == "run_number" and liveRunNumber is not None
        self.run.getProperty.return_value.value = liveRunNumber
        self.ws = mock.Mock()
        self.ws.getRun.return_value = self.run
        self.mutableRun = {}
        self.ws.mutableRun.return_value = self.mutableRun

        self.workspaces = mock.Mock()
        self.workspaces.getWorkspaceForName.return_value = self.ws

        result = {"result": True, "loader": LiveListenerSource.LOADER, "workspace": "out"}
        if runStatus is not None:
            result["runStatus"] = runStatus
        self.grocer = mock.Mock()
        self.grocer.executeRecipe.return_value = result

        self.source = LiveListenerSource(self.dataService, self.grocer, self.workspaces)

    def loaderArgs(self):
        return json.loads(self.grocer.executeRecipe.call_args.kwargs["loaderArgs"])


def test_kind():
    source = Harness().source
    assert source.kind == DataSourceKind.LIVE
    assert source.provides == Resolution.NATIVE
    assert source.requires is None
    assert not source.cacheable


@pytest.mark.parametrize("connected", [True, False])
def test_available(connected):
    assert Harness(connected=connected).source.available(item()) is connected


def test_no_connection():
    with pytest.raises(ConnectionError, match="no live-data connection is available"):
        Harness(connected=False).source.load(item(), "out")


def test_load():
    h = Harness()
    data = h.source.load(item(LIVE_ARGS), "out")
    assert data.workspace == "out"
    assert data.loader == LiveListenerSource.LOADER
    assert data.provenance == [DataSourceKind.LIVE]
    h.grocer.executeRecipe.assert_called_once()
    assert h.grocer.executeRecipe.call_args.kwargs["workspace"] == "out"
    assert h.grocer.executeRecipe.call_args.kwargs["loader"] == LiveListenerSource.LOADER
    loaderArgs = h.loaderArgs()
    assert loaderArgs["Facility"] == Config["liveData.facility.name"]
    assert loaderArgs["Instrument"] == Config["liveData.instrument.name"]
    assert loaderArgs["PreserveEvents"] is True
    h.workspaces.deleteWorkspaceUnconditional.assert_not_called()


def test_start_time_from_duration():
    h = Harness()
    before = datetime.datetime.utcnow()
    h.source.load(item(LIVE_ARGS), "out")
    after = datetime.datetime.utcnow()
    startTime = datetime.datetime.fromisoformat(h.loaderArgs()["StartTime"])
    assert before - LIVE_ARGS.duration <= startTime <= after - LIVE_ARGS.duration


@pytest.mark.parametrize("liveDataArgs", [None, LiveDataArgs(duration=datetime.timedelta(0))])
def test_start_time_from_start_of_run(liveDataArgs):
    h = Harness()
    h.source.load(item(liveDataArgs), "out")
    assert h.loaderArgs()["StartTime"] == RunMetadata.FROM_START_ISO8601


def test_fallback_warns():
    from snapred.backend.data.source import LiveListenerSource as module

    with mock.patch.object(module, "logger") as mockLogger:
        Harness().source.load(item(), "out")
    mockLogger.warning.assert_called_once()
    assert "trying live-data fallback" in mockLogger.warning.call_args.args[0]


def test_requested_does_not_warn():
    from snapred.backend.data.source import LiveListenerSource as module

    with mock.patch.object(module, "logger") as mockLogger:
        Harness().source.load(item(LIVE_ARGS), "out")
    mockLogger.warning.assert_not_called()


def test_run_status_running():
    h = Harness(runStatus=RunStatus.RUNNING.value)
    assert h.source.load(item(LIVE_ARGS), "out").workspace == "out"
    h.workspaces.deleteWorkspaceUnconditional.assert_not_called()


@pytest.mark.parametrize(
    ("status", "endRunNumber"),
    [(RunStatus.PAUSED, RUN_NUMBER), (RunStatus.STOPPED, "0"), (RunStatus.ERROR, None)],
)
def test_run_status_not_running_when_requested(status, endRunNumber):
    h = Harness(runStatus=status.value)
    with pytest.raises(LiveDataState) as e:
        h.source.load(item(LIVE_ARGS), "out")
    h.workspaces.deleteWorkspaceUnconditional.assert_called_once_with("out")
    if endRunNumber is not None:
        assert e.value.model.endRunNumber == endRunNumber


def test_run_status_not_running_in_fallback():
    h = Harness(runStatus=RunStatus.PAUSED.value)
    with pytest.raises(RuntimeError, match="there is a problem with the live-data run"):
        h.source.load(item(), "out")
    h.workspaces.deleteWorkspaceUnconditional.assert_called_once_with("out")


def test_run_status_invalid_is_ignored():
    from snapred.backend.data.source import LiveListenerSource as module

    h = Harness(runStatus="NOT_A_STATUS")
    with mock.patch.object(module, "logger") as mockLogger:
        assert h.source.load(item(LIVE_ARGS), "out").workspace == "out"
    mockLogger.debug.assert_called_once()
    h.workspaces.deleteWorkspaceUnconditional.assert_not_called()


def test_run_transition_when_requested():
    h = Harness(liveRunNumber="54321")
    with pytest.raises(LiveDataState):
        h.source.load(item(LIVE_ARGS), "out")
    h.workspaces.deleteWorkspaceUnconditional.assert_called_once_with("out")


def test_not_the_live_run_in_fallback():
    h = Harness(liveRunNumber="54321")
    with pytest.raises(RuntimeError, match="nor is it the live-data run"):
        h.source.load(item(), "out")
    h.workspaces.deleteWorkspaceUnconditional.assert_called_once_with("out")


def test_run_number_not_set_when_requested_is_filled_in():
    h = Harness(liveRunNumber=None)
    h.source.load(item(LIVE_ARGS), "out")
    assert h.mutableRun["run_number"] == RUN_NUMBER
    h.workspaces.deleteWorkspaceUnconditional.assert_not_called()


def test_run_number_not_set_in_fallback():
    h = Harness(liveRunNumber=None)
    with pytest.raises(RuntimeError, match="nor is it the live-data run"):
        h.source.load(item(), "out")
    assert h.mutableRun == {}
    h.workspaces.deleteWorkspaceUnconditional.assert_called_once_with("out")
