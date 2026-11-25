from unittest import mock

import pytest

from snapred.backend.dao.ingredients.GroceryListItem import GroceryListItem
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
def test_registries(resolution):
    registry = sources().registries[(Product.GROUPING, resolution)]
    assert (registry.product, registry.resolution) == (Product.GROUPING, resolution)
    assert set(registry.sources) == {K.GROUPING_FILE}
    assert [source.kind for source in registry.ordered()] == [K.GROUPING_FILE]


def test_grouping_cache_is_shared_by_the_resolutions():
    s = sources()
    assert isinstance(s.groupingCache, GroupingDataCache)
    assert s.resolver(Product.GROUPING, item(False)).cache is s.groupingCache
    assert s.resolver(Product.GROUPING, item(True)).cache is s.groupingCache


@pytest.mark.parametrize("useLiteMode", [False, True])
def test_fetchGrouping_uses_the_resolver(useLiteMode):
    s = sources()
    resolver = s.resolver(Product.GROUPING, item(useLiteMode))
    assert resolver.resolution == Resolution.fromLiteMode(useLiteMode)
    with mock.patch.object(resolver, "resolve", return_value=mock.sentinel.data) as resolve:
        assert s.fetchGrouping(item(useLiteMode)) is mock.sentinel.data
    resolve.assert_called_once_with(item(useLiteMode))
