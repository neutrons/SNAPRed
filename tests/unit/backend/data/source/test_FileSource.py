import datetime
from unittest import mock

import pytest
from util.Config_helpers import Config_override

from snapred.backend.dao.ingredients.GroceryListItem import GroceryListItem, LiveDataArgs
from snapred.backend.data.source.DataSourceKind import DataSourceKind
from snapred.backend.data.source.FileSource import FileSource
from snapred.backend.data.source.Resolution import Resolution

ThisModule = "snapred.backend.data.source.FileSource."


def item(loader: str = "", liveDataArgs=None) -> GroceryListItem:
    return GroceryListItem(
        workspaceType="neutron", runNumber="12345", useLiteMode=False, loader=loader, liveDataArgs=liveDataArgs
    )


@pytest.fixture
def filePath(tmp_path):
    path = tmp_path / "SNAP_12345.nxs.h5"
    path.touch()
    return path


def source(resolution=Resolution.NATIVE, path=None):
    dataService = mock.Mock()
    dataService.createNeutronFilePath.return_value = path
    grocer = mock.Mock()
    grocer.executeRecipe.side_effect = lambda filename, workspace, loader, **kwargs: {  # noqa: ARG005
        "result": True,
        "loader": loader or "chosenByAlgorithm",
        "workspace": workspace,
    }
    return FileSource(resolution, dataService, grocer)


@pytest.mark.parametrize(
    ("resolution", "kind"),
    [(Resolution.NATIVE, DataSourceKind.NEXUS_FILE), (Resolution.LITE, DataSourceKind.LITE_FILE)],
)
def test_kind(resolution, kind):
    src = source(resolution)
    assert src.kind == kind
    assert src.provides == resolution
    assert src.cacheable


def test_available(filePath):
    assert source(path=filePath).available(item())


def test_not_available_without_file(tmp_path):
    assert not source(path=tmp_path / "missing.nxs.h5").available(item())


def test_not_available_without_IPTS():
    assert not source(path=None).available(item())


def test_not_available_when_live_data_is_requested(filePath):
    assert not source(path=filePath).available(item(liveDataArgs=LiveDataArgs(duration=datetime.timedelta(0))))


def test_path_uses_resolution(filePath):
    src = source(Resolution.LITE, filePath)
    src.available(item())
    src.dataService.createNeutronFilePath.assert_called_once_with("12345", True)


def test_load_with_specified_loader(filePath):
    src = source(path=filePath)
    data = src.load(item(loader="LoadNexusProcessed"), "out")
    src.grocer.executeRecipe.assert_called_once_with(str(filePath), "out", "LoadNexusProcessed", loaderArgs="{}")
    assert data.workspace == "out"
    assert data.loader == "LoadNexusProcessed"
    assert data.provenance == [DataSourceKind.NEXUS_FILE]
    assert data.derivedFrom is None
    assert data.resolution == Resolution.NATIVE


@mock.patch(ThisModule + "FileLoaderRegistry")
def test_load_event_data_with_a_single_bin(mockRegistry, filePath):
    mockRegistry.Instance.return_value.chooseLoader.return_value.name.return_value = "LoadEventNexus"
    src = source(path=filePath)
    with Config_override("nexus.dataFormat.event", True):
        src.load(item(), "out")
    src.grocer.executeRecipe.assert_called_once_with(
        str(filePath), "out", "LoadEventNexus", loaderArgs='{"NumberOfBins": 1}'
    )


@mock.patch(ThisModule + "FileLoaderRegistry")
def test_load_other_format_when_event_data_is_expected(mockRegistry, filePath):
    mockRegistry.Instance.return_value.chooseLoader.return_value.name.return_value = "LoadNexusProcessed"
    src = source(path=filePath)
    with Config_override("nexus.dataFormat.event", True):
        src.load(item(), "out")
    src.grocer.executeRecipe.assert_called_once_with(str(filePath), "out", "LoadNexusProcessed", loaderArgs="{}")


@mock.patch(ThisModule + "FileLoaderRegistry")
def test_load_without_loader_when_event_data_is_not_expected(mockRegistry, filePath):
    src = source(path=filePath)
    with Config_override("nexus.dataFormat.event", False):
        data = src.load(item(), "out")
    mockRegistry.Instance.assert_not_called()
    src.grocer.executeRecipe.assert_called_once_with(str(filePath), "out", "", loaderArgs="{}")
    assert data.loader == "chosenByAlgorithm"
