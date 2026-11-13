from unittest import mock

import pytest
from mantid.simpleapi import CreateSingleValuedWorkspace, DeleteWorkspace, mtd
from util.Config_helpers import Config_override
from util.dao import DAOFactory
from util.instrument_helpers import getInstrumentLogDescriptors

from snapred.backend.data.source.InstrumentDonors import InstrumentDonors
from snapred.backend.data.source.NeutronDataCache import NeutronDataCache
from snapred.backend.data.source.WorkspaceOperations import WorkspaceOperations
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper
from snapred.meta.Config import Resource
from snapred.meta.InternalConstants import ReservedRunNumber

RUN_NUMBER = "555"
STATE_ID = "0123456789abcdef"


@pytest.fixture
def donors():
    workspaces = WorkspaceOperations(MantidSnapper(None, "Utensils"))
    cache = NeutronDataCache(workspaces)
    dataService = mock.Mock()
    dataService.generateStateId.return_value = (STATE_ID, DAOFactory.real_detector_state)
    instance = InstrumentDonors(dataService, workspaces, cache)
    yield instance
    for ws in [
        *instance.cachedWorkspaces(),
        cache.rawWorkspace(RUN_NUMBER, False),
        cache.rawWorkspace(RUN_NUMBER, True),
    ]:
        if mtd.doesExist(ws):
            DeleteWorkspace(ws)


def test_cached_neutron_data_is_the_donor(donors):
    raw = donors.neutronDataCache.rawWorkspace(RUN_NUMBER, False)
    CreateSingleValuedWorkspace(OutputWorkspace=raw)
    donors.neutronDataCache.loadedRuns[(RUN_NUMBER, False)] = 0
    assert donors.fetch(RUN_NUMBER, False) == raw
    assert donors.loadedInstruments == {(STATE_ID, False): raw}


def test_cached_donor_is_reused(donors):
    ws = mtd.unique_hidden_name()
    CreateSingleValuedWorkspace(OutputWorkspace=ws)
    donors.loadedInstruments[(STATE_ID, False)] = ws
    assert donors.fetch(RUN_NUMBER, False) == ws
    DeleteWorkspace(ws)


@pytest.mark.parametrize("useLiteMode", [False, True])
def test_empty_instrument_donor(donors, useLiteMode):
    instrumentFile = Resource.getPath(
        "inputs/testInstrument/fakeSNAPLite.xml" if useLiteMode else "inputs/testInstrument/fakeSNAP_Definition.xml"
    )
    key = "instrument.lite.definition.file" if useLiteMode else "instrument.native.definition.file"
    with Config_override(key, instrumentFile):
        ws = donors.fetch(RUN_NUMBER, useLiteMode)
    assert mtd.doesExist(ws)
    assert donors.loadedInstruments == {(STATE_ID, useLiteMode): ws}
    # The instrument parameters are set from the detector state.
    logs = getInstrumentLogDescriptors(DAOFactory.real_detector_state)
    run = mtd[ws].getRun()
    for name in logs["logNames"]:
        assert run.hasProperty(name)


def test_reserved_run_number_keeps_the_unmodified_instrument(donors):
    runNumber = ReservedRunNumber.NATIVE
    with Config_override(
        "instrument.native.definition.file", Resource.getPath("inputs/testInstrument/fakeSNAP_Definition.xml")
    ):
        ws = donors.fetch(runNumber, False)
    logs = getInstrumentLogDescriptors(DAOFactory.real_detector_state)
    assert not any(mtd[ws].getRun().hasProperty(name) for name in logs["logNames"])


def test_updateFromADS_adds_cached_neutron_data(donors):
    raw = donors.neutronDataCache.rawWorkspace(RUN_NUMBER, True)
    CreateSingleValuedWorkspace(OutputWorkspace=raw)
    donors.updateFromADS(RUN_NUMBER, True, (STATE_ID, True))
    assert donors.loadedInstruments == {(STATE_ID, True): raw}


def test_updateFromADS_drops_a_deleted_donor(donors):
    donors.loadedInstruments[(STATE_ID, False)] = "not_a_workspace"
    donors.updateFromADS(RUN_NUMBER, False, (STATE_ID, False))
    assert donors.loadedInstruments == {}


def test_rebuild(donors):
    ws = mtd.unique_hidden_name()
    CreateSingleValuedWorkspace(OutputWorkspace=ws)
    donors.loadedInstruments = {(STATE_ID, False): ws, (STATE_ID, True): "not_a_workspace"}
    donors.rebuild()
    assert donors.loadedInstruments == {(STATE_ID, False): ws}
    assert donors.cachedWorkspaces() == [ws]
    DeleteWorkspace(ws)
