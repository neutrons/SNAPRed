from unittest import mock

import pytest
from util.Config_helpers import Config_override

from snapred.backend.dao.ingredients.GroceryListItem import GroceryListItem
from snapred.backend.dao.WorkspaceMetadata import WorkspaceMetadata
from snapred.backend.data.source.DataSourceKind import DataSourceKind
from snapred.backend.data.source.LiteConversionSource import LiteConversionSource
from snapred.backend.data.source.Resolution import Resolution
from snapred.backend.data.source.SourcedData import SourcedData

ThisModule = "snapred.backend.data.source.LiteConversionSource."


@pytest.fixture
def mockLiteDataService():
    with mock.patch("snapred.backend.service.LiteDataService.LiteDataService") as mockClass:
        mockClass.return_value.createLiteData.return_value = ("ws", 0.004)
        yield mockClass


@pytest.fixture
def mockMetadataService():
    with mock.patch(ThisModule + "WorkspaceMetadataService") as mockClass:
        yield mockClass.return_value


def item() -> GroceryListItem:
    return GroceryListItem(workspaceType="neutron", runNumber="12345", useLiteMode=True)


def nativeData(*kinds) -> SourcedData:
    return SourcedData(workspace="native", loader="LoadNexusProcessed", provenance=list(kinds))


def test_kind():
    source = LiteConversionSource(mock.Mock())
    assert source.kind == DataSourceKind.LITE_CONVERSION
    assert source.provides == Resolution.LITE
    assert source.requires == Resolution.NATIVE
    assert source.cacheable
    assert source.available(item())


@pytest.mark.parametrize(
    ("provenance", "exports"),
    [
        ([DataSourceKind.NEXUS_FILE], True),
        ([DataSourceKind.LIVE], False),
        # unknown provenance: treated as file data
        ([], True),
    ],
)
def test_exportsFrom(provenance, exports):
    assert LiteConversionSource.exportsFrom(nativeData(*provenance)) is exports


@pytest.mark.parametrize(
    ("kind", "export"),
    [(DataSourceKind.NEXUS_FILE, True), (DataSourceKind.LIVE, False)],
)
def test_load(mockLiteDataService, kind, export):
    workspaces = mock.Mock()
    source = LiteConversionSource(workspaces)
    inputData = nativeData(kind)
    data = source.load(item(), "lite", inputData)

    workspaces.getCloneOfWorkspace.assert_called_once_with("native", "lite")
    mockLiteDataService.return_value.createLiteData.assert_called_once_with("lite", "lite", export=export)
    assert data.workspace == "lite"
    assert data.loader == "LoadNexusProcessed"
    assert data.provenance == [kind, DataSourceKind.LITE_CONVERSION]
    assert data.inputData is inputData


def test_load_without_input():
    with pytest.raises(RuntimeError, match="has no input"):
        LiteConversionSource(mock.Mock()).load(item(), "lite")


def test_convert_without_tolerance_tag(mockLiteDataService, mockMetadataService):
    with Config_override("constants.LiteDataCreationAlgo.toggleCompressionTolerance", False):
        LiteConversionSource(mock.Mock()).convert("ws")
    mockLiteDataService.return_value.createLiteData.assert_called_once_with("ws", "ws", export=True)
    mockMetadataService.writeWorkspaceMetadata.assert_not_called()


def test_convert_with_tolerance_tag(mockLiteDataService, mockMetadataService):
    with Config_override("constants.LiteDataCreationAlgo.toggleCompressionTolerance", True):
        LiteConversionSource(mock.Mock()).convert("ws", export=False)
    mockLiteDataService.return_value.createLiteData.assert_called_once_with("ws", "ws", export=False)
    mockMetadataService.writeWorkspaceMetadata.assert_called_once_with(
        "ws", WorkspaceMetadata(liteDataCompressionTolerance=0.004)
    )
