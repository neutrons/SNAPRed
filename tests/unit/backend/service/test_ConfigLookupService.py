import unittest.mock as mock

from snapred.backend.service.ConfigLookupService import ConfigLookupService as Service


def test_getGroupingMap():
    service = Service()
    service.dataFactoryService = mock.Mock()
    actual = service.getGroupingMap(mock.Mock())
    assert actual == service.dataFactoryService.getGroupingMap.return_value


def test_getSamplePaths():
    service = Service()
    service.dataFactoryService = mock.Mock()
    actual = service.getSamplePaths()
    assert actual == service.dataFactoryService.getSamplePaths.return_value


def test_getConfigs():
    service = Service()
    service.dataFactoryService = mock.Mock()
    runs = [mock.MagicMock()]
    actual = service.getConfigs(runs)
    assert actual == {runs[0].runNumber: service.dataFactoryService.getReductionState.return_value}
