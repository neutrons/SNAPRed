from unittest import mock

import pytest

from snapred.backend.dao.ingredients.GroceryListItem import GroceryListItem
from snapred.backend.data.source.DataCache import ResidentDataCache
from snapred.backend.data.source.DataSourceKind import DataSourceKind
from snapred.backend.data.source.GroupingSources import GroupingDataCache
from snapred.backend.data.source.Product import Product
from snapred.backend.data.source.ReductionDataSources import ReductionDataSources
from snapred.backend.data.source.Resolution import Resolution

K = DataSourceKind


def sources() -> ReductionDataSources:
    return ReductionDataSources(mock.Mock(), mock.Mock(), mock.Mock(), mock.Mock())


def item(useLiteMode: bool) -> GroceryListItem:
    return GroceryListItem(workspaceType="grouping", runNumber="1", useLiteMode=useLiteMode, groupingScheme="Column")


@pytest.mark.parametrize("resolution", list(Resolution))
@pytest.mark.parametrize(
    ("product", "kind"),
    [(Product.GROUPING, K.GROUPING_FILE), (Product.REDUCTION_PIXEL_MASK, K.REDUCTION_RECORD)],
)
def test_registries(resolution, product, kind):
    registry = sources().registries[(product, resolution)]
    assert (registry.product, registry.resolution) == (product, resolution)
    assert set(registry.sources) == {kind}
    assert [source.kind for source in registry.ordered()] == [kind]


def test_grouping_cache_is_shared_by_the_resolutions():
    s = sources()
    assert isinstance(s.groupingCache, GroupingDataCache)
    assert s.resolver(Product.GROUPING, item(False)).cache is s.groupingCache
    assert s.resolver(Product.GROUPING, item(True)).cache is s.groupingCache


def test_reduction_pixel_mask_is_reused_while_resident():
    s = sources()
    for useLiteMode in (False, True):
        cache = s.resolver(Product.REDUCTION_PIXEL_MASK, item(useLiteMode)).cache
        assert type(cache) is ResidentDataCache


@pytest.mark.parametrize(
    ("method", "product"),
    [("fetchGrouping", Product.GROUPING), ("fetchReductionPixelMask", Product.REDUCTION_PIXEL_MASK)],
)
@pytest.mark.parametrize("useLiteMode", [False, True])
def test_fetch_uses_the_resolver(method, product, useLiteMode):
    s = sources()
    resolver = s.resolver(product, item(useLiteMode))
    assert resolver.resolution == Resolution.fromLiteMode(useLiteMode)
    with mock.patch.object(resolver, "resolve", return_value=mock.sentinel.data) as resolve:
        assert getattr(s, method)(item(useLiteMode)) is mock.sentinel.data
    resolve.assert_called_once_with(item(useLiteMode))
