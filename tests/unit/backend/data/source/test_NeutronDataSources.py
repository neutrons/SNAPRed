from unittest import mock

from snapred.backend.dao.ingredients.GroceryListItem import GroceryListItem
from snapred.backend.data.source.DataSourceKind import DataSourceKind
from snapred.backend.data.source.NeutronDataSources import NeutronDataSources
from snapred.backend.data.source.Resolution import Resolution

K = DataSourceKind


def sources() -> NeutronDataSources:
    return NeutronDataSources(mock.Mock(), mock.Mock(), mock.Mock())


def item(useLiteMode: bool) -> GroceryListItem:
    return GroceryListItem(workspaceType="neutron", runNumber="12345", useLiteMode=useLiteMode)


def test_registries():
    s = sources()
    assert s.native.resolution == Resolution.NATIVE
    assert set(s.native.sources) == {K.REGISTERED, K.NEXUS_FILE, K.LIVE}
    assert s.lite.resolution == Resolution.LITE
    assert set(s.lite.sources) == {K.LITE_FILE, K.LITE_CONVERSION}


def test_lite_conversion_uses_the_native_registry_and_the_shared_cache():
    s = sources()
    conversion = s.lite.sources[K.LITE_CONVERSION]
    assert conversion.nativeData.sources is s.native
    assert conversion.nativeData.cache is s.cache


def test_resolver_is_chosen_by_resolution():
    s = sources()
    assert s.resolver(item(False)).sources is s.native
    assert s.resolver(item(True)).sources is s.lite
    assert s.resolver(item(False)).cache is s.cache
    assert s.resolver(item(True)).cache is s.cache


def test_fetches_delegate_to_the_resolver():
    s = sources()
    for useLiteMode in (False, True):
        resolver = s.resolver(item(useLiteMode))
        with (
            mock.patch.object(resolver, "fetchSingleUse", return_value="single") as fetchSingleUse,
            mock.patch.object(resolver, "fetchCached", return_value="cached") as fetchCached,
        ):
            assert s.fetchSingleUse(item(useLiteMode)) == "single"
            assert s.fetchCached(item(useLiteMode)) == "cached"
        fetchSingleUse.assert_called_once_with(item(useLiteMode))
        fetchCached.assert_called_once_with(item(useLiteMode))


def test_convertToLiteMode():
    s = sources()
    conversion = s.lite.sources[K.LITE_CONVERSION]
    with mock.patch.object(conversion, "convert") as convert:
        s.convertToLiteMode("ws", export=False)
    convert.assert_called_once_with("ws", export=False)
