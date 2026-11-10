from unittest import mock

import pytest

from snapred.backend.data.source.NeutronDataCache import NeutronDataCache
from snapred.backend.data.source.NeutronWorkspaceNames import NeutronWorkspaceNames as names

RUN_NUMBER = "12345"


@pytest.fixture
def ads():
    """The names of the workspaces in a fake ADS."""
    return set()


@pytest.fixture
def cache(ads):
    workspaces = mock.Mock()
    workspaces.workspaceDoesExist.side_effect = lambda name: name in ads
    workspaces.deleteWorkspaceUnconditional.side_effect = lambda name: ads.discard(name)
    return NeutronDataCache(workspaces)


KEY = (RUN_NUMBER, True)


def test_key():
    assert NeutronDataCache.key(RUN_NUMBER, True) == KEY


def test_updateFromADS_not_cached_not_in_ADS(cache):
    cache.updateFromADS(*KEY)
    assert cache.loadedRuns == {}


def test_updateFromADS_cached_and_in_ADS(cache, ads):
    ads.add(names.raw(*KEY))
    cache.loadedRuns[KEY] = 3
    cache.updateFromADS(*KEY)
    assert cache.loadedRuns == {KEY: 3}


def test_updateFromADS_cached_not_in_ADS(cache):
    cache.loadedRuns[KEY] = 3
    cache.updateFromADS(*KEY)
    assert cache.loadedRuns == {}
    assert not cache.contains(KEY)


def test_updateFromADS_in_ADS_not_cached(cache, ads):
    ads.add(names.raw(*KEY))
    cache.updateFromADS(*KEY)
    assert cache.loadedRuns == {KEY: 0}
    assert cache.contains(KEY)


def test_rebuild(cache, ads):
    other = (RUN_NUMBER, False)
    ads.add(names.raw(*KEY))
    cache.loadedRuns.update({KEY: 1, other: 1})
    cache.rebuild()
    assert cache.loadedRuns == {KEY: 1}


def test_cachedWorkspaces(cache):
    cache.loadedRuns.update({KEY: 1, (RUN_NUMBER, False): 0})
    assert sorted(cache.cachedWorkspaces()) == sorted([names.raw(*KEY), names.raw(RUN_NUMBER, False)])


def test_recordUncacheable(cache):
    cache.recordUncacheable(KEY)
    assert cache.loadedRuns == {KEY: 0}
    assert cache.uncacheableKeys == [KEY]


def test_clearUncacheable(cache, ads):
    other = (RUN_NUMBER, False)
    ads.update({names.raw(*KEY), names.raw(*other)})
    cache.loadedRuns[other] = 1
    cache.recordUncacheable(KEY)
    cache.clearUncacheable()
    assert cache.loadedRuns == {other: 1}
    assert cache.uncacheableKeys == []
    assert ads == {names.raw(*other)}


def test_clearUncacheable_entry_already_dropped_or_recorded_twice(cache):
    cache.recordUncacheable(KEY)
    cache.recordUncacheable(KEY)
    cache.updateFromADS(*KEY)  # no raw workspace: the entry is dropped
    cache.clearUncacheable()
    assert cache.loadedRuns == {}
    assert cache.uncacheableKeys == []
