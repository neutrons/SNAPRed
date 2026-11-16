from unittest import mock

import pytest

from snapred.backend.dao.ingredients.GroceryListItem import GroceryListItem
from snapred.backend.data.source.CalibrationDataSources import CalibrationDataSources
from snapred.backend.data.source.DataCache import SingleEntryDataCache
from snapred.backend.data.source.DataSourceKind import DataSourceKind
from snapred.backend.data.source.Product import Product
from snapred.backend.data.source.Resolution import Resolution

K = DataSourceKind


def sources() -> CalibrationDataSources:
    return CalibrationDataSources(mock.Mock(), mock.Mock(), mock.Mock(), mock.Mock())


def item(useLiteMode: bool) -> GroceryListItem:
    return GroceryListItem(
        workspaceType="normalization", runNumber="1", useLiteMode=useLiteMode, state="s", normCalVersion=1
    )


@pytest.mark.parametrize("resolution", list(Resolution))
def test_registries(resolution):
    s = sources()
    for product in (Product.DIFFCAL_TABLE, Product.DIFFCAL_MASK):
        registry = s.registries[(product, resolution)]
        assert (registry.product, registry.resolution) == (product, resolution)
        assert set(registry.sources) == {K.CALIBRATION_FILE, K.CALIBRATION_RECORD}
    assert set(s.registries[(Product.NORMALIZATION, resolution)].sources) == {K.NORMALIZATION_RECORD}


def test_normalization_cache_is_single_entry():
    s = sources()
    assert isinstance(s.normalizationCache, SingleEntryDataCache)
    assert s.resolver(Product.NORMALIZATION, item(False)).cache is s.normalizationCache
    assert s.resolver(Product.DIFFCAL_TABLE, item(False)).cache is not s.normalizationCache


@pytest.mark.parametrize(
    ("method", "product"),
    [
        ("fetchDiffCalTable", Product.DIFFCAL_TABLE),
        ("fetchDiffCalMask", Product.DIFFCAL_MASK),
        ("fetchNormalization", Product.NORMALIZATION),
    ],
)
@pytest.mark.parametrize("useLiteMode", [False, True])
def test_fetch_uses_the_resolver(method, product, useLiteMode):
    s = sources()
    resolver = s.resolver(product, item(useLiteMode))
    assert resolver.resolution == Resolution.fromLiteMode(useLiteMode)
    with mock.patch.object(resolver, "resolve", return_value=mock.sentinel.data) as resolve:
        assert getattr(s, method)(item(useLiteMode)) is mock.sentinel.data
    resolve.assert_called_once_with(item(useLiteMode))
