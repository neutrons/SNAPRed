from unittest import mock

import pytest
from util.Config_helpers import Config_override

from snapred.backend.dao.ingredients.GroceryListItem import GroceryListItem
from snapred.backend.dao.WorkspaceMetadata import WorkspaceMetadata
from snapred.backend.data.source.DataSourceKind import DataSourceKind
from snapred.backend.data.source.DataSourceRegistry import DataSourceRegistry
from snapred.backend.data.source.LiteConversionSource import LiteConversionSource
from snapred.backend.data.source.NeutronDataCache import NeutronDataCache
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
    return SourcedData(
        workspace="native", loader="LoadNexusProcessed", resolution=Resolution.NATIVE, provenance=list(kinds)
    )


def nativeSources():
    return DataSourceRegistry(Resolution.NATIVE, [])


def source(workspaces=None) -> LiteConversionSource:
    return LiteConversionSource(nativeSources(), NeutronDataCache(mock.Mock()), workspaces or mock.Mock())


def test_kind():
    src = source()
    assert src.kind == DataSourceKind.LITE_CONVERSION
    assert src.provides == Resolution.LITE
    assert src.cacheable
    assert src.available(item())


def test_native_resolver_uses_the_native_sources():
    sources = nativeSources()
    cache = NeutronDataCache(mock.Mock())
    src = LiteConversionSource(sources, cache, mock.Mock())
    assert src.nativeData.sources is sources
    assert src.nativeData.cache is cache
    assert src.nativeData.resolution == Resolution.NATIVE


def test_requires_native_sources():
    with pytest.raises(RuntimeError, match="requires native sources, not lite"):
        LiteConversionSource(DataSourceRegistry(Resolution.LITE, []), NeutronDataCache(mock.Mock()), mock.Mock())


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
    src = source(workspaces)
    native = nativeData(kind)
    src.nativeData = mock.Mock(resolve=mock.Mock(return_value=native))
    data = src.load(item(), "lite")

    src.nativeData.resolve.assert_called_once_with(item())
    workspaces.getCloneOfWorkspace.assert_called_once_with("native", "lite")
    mockLiteDataService.return_value.createLiteData.assert_called_once_with("lite", "lite", export=export)
    assert data.workspace == "lite"
    assert data.loader == "LoadNexusProcessed"
    assert data.provenance == [kind, DataSourceKind.LITE_CONVERSION]
    assert data.resolution == Resolution.LITE
    assert data.derivedFrom is native


def test_convert_without_tolerance_tag(mockLiteDataService, mockMetadataService):
    with Config_override("constants.LiteDataCreationAlgo.toggleCompressionTolerance", False):
        source().convert("ws")
    mockLiteDataService.return_value.createLiteData.assert_called_once_with("ws", "ws", export=True)
    mockMetadataService.writeWorkspaceMetadata.assert_not_called()


def test_convert_with_tolerance_tag(mockLiteDataService, mockMetadataService):
    with Config_override("constants.LiteDataCreationAlgo.toggleCompressionTolerance", True):
        source().convert("ws", export=False)
    mockLiteDataService.return_value.createLiteData.assert_called_once_with("ws", "ws", export=False)
    mockMetadataService.writeWorkspaceMetadata.assert_called_once_with(
        "ws", WorkspaceMetadata(liteDataCompressionTolerance=0.004)
    )
