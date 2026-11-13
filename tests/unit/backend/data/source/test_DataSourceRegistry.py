import pytest
from util.Config_helpers import Config_override

from snapred.backend.data.source.DataSource import DataSource
from snapred.backend.data.source.DataSourceKind import DataSourceKind
from snapred.backend.data.source.DataSourceRegistry import DataSourceRegistry
from snapred.backend.data.source.Resolution import Resolution

K = DataSourceKind


class FakeSource(DataSource):
    def __init__(self, kind, provides):
        super().__init__(provides=provides)
        self._kind = kind

    @property
    def kind(self):
        return self._kind

    def available(self, item):  # noqa: ARG002
        return True

    def load(self, item, outputWorkspace):  # noqa: ARG002
        return self._sourced(outputWorkspace, "fake")


def nativeSources():
    return [
        FakeSource(K.REGISTERED, Resolution.NATIVE),
        FakeSource(K.NEXUS_FILE, Resolution.NATIVE),
        FakeSource(K.LIVE, Resolution.NATIVE),
    ]


def test_sources_are_keyed_by_kind():
    sources = nativeSources()
    registry = DataSourceRegistry(Resolution.NATIVE, sources)
    assert registry.resolution == Resolution.NATIVE
    assert registry.sources == {K.REGISTERED: sources[0], K.NEXUS_FILE: sources[1], K.LIVE: sources[2]}


def test_source_of_another_resolution_is_rejected():
    with pytest.raises(RuntimeError, match=r"the 'liteFile' source provides lite data, not native data"):
        DataSourceRegistry(Resolution.NATIVE, [*nativeSources(), FakeSource(K.LITE_FILE, Resolution.LITE)])


@pytest.mark.parametrize("order", [["nexusFile", "live"], ["live", "nexusFile"], ["live"]])
def test_ordered_follows_the_configuration(order):
    registry = DataSourceRegistry(Resolution.NATIVE, nativeSources())
    with Config_override("neutronData.sources.native", order):
        assert [source.kind for source in registry.ordered()] == [K(kind) for kind in order]


def test_ordered_rejects_a_source_not_in_the_registry():
    registry = DataSourceRegistry(Resolution.NATIVE, nativeSources())
    with Config_override("neutronData.sources.native", ["nexusFile", "liteFile"]):
        with pytest.raises(RuntimeError, match=r"lists 'liteFile', which is not a source of native data"):
            registry.ordered()


def test_configured_order():
    # The order in `application.yml`.
    assert [source.kind for source in DataSourceRegistry(Resolution.NATIVE, nativeSources()).ordered()] == [
        K.REGISTERED,
        K.NEXUS_FILE,
        K.LIVE,
    ]
