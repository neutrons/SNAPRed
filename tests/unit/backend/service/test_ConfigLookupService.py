import unittest.mock as mock

from snapred.backend.dao.request.UpdateCycleRequest import UpdateCycleRequest
from snapred.backend.dao.state.Cycle import Cycle
from snapred.backend.service.ConfigLookupService import ConfigLookupService as Service


def test_getGroupingMap():
    service = Service()
    service.dataFactoryService = mock.Mock()
    actual = service.getGroupingMap(mock.Mock())
    assert actual == service.dataFactoryService.getGroupingMap.return_value


def test_getSampleFilePaths():
    service = Service()
    service.dataFactoryService = mock.Mock()
    actual = service.getSampleFilePaths()
    assert actual == service.dataFactoryService.getSampleFilePaths.return_value


def test_getConfigs():
    service = Service()
    service.dataFactoryService = mock.Mock()
    runs = [mock.MagicMock()]
    actual = service.getConfigs(runs)
    assert actual == {runs[0].runNumber: service.dataFactoryService.getReductionState.return_value}


def test_updateCycle():
    service = Service()
    service.dataFactoryService = mock.Mock()
    cycle = Cycle(cycleID="2024-A", startDate="2024-01-01", stopDate="2024-06-30", firstRun=100)
    request = UpdateCycleRequest(runNumber="12345", cycle=cycle, appliesTo=">=12345", author="testAuthor")

    actual = service.updateCycle(request)

    service.dataFactoryService.updateInstrumentConfigCycle.assert_called_once_with(
        runNumber="12345",
        cycle=cycle,
        appliesTo=">=12345",
        author="testAuthor",
    )
    assert actual == service.dataFactoryService.updateInstrumentConfigCycle.return_value
