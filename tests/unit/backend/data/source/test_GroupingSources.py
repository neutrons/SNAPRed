from types import SimpleNamespace
from unittest import mock

import pytest

from snapred.backend.dao.ingredients.GroceryListItem import GroceryListItem
from snapred.backend.data.source.DataSourceKind import DataSourceKind
from snapred.backend.data.source.GroupingSources import (
    LOADER,
    GroupingDataCache,
    GroupingFileSource,
    groupingWorkspaceName,
)
from snapred.backend.data.source.Product import Product
from snapred.backend.data.source.Resolution import Resolution
from snapred.meta.Config import Config
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceNameGenerator as wng

STATE = "0123456789abcdef"


def item(scheme="Column", runNumber="12345", useLiteMode=False, **kwargs) -> GroceryListItem:
    return GroceryListItem(
        workspaceType="grouping", runNumber=runNumber, useLiteMode=useLiteMode, groupingScheme=scheme, **kwargs
    )


class FakeADS:
    def __init__(self, *names):
        self.names = set(names)

    def workspaceDoesExist(self, name):
        return name in self.names


def dataService(definitions=None):
    service = mock.Mock()
    service.generateStateId.return_value = (STATE, mock.sentinel.detectorState)
    service.readGroupingMap.return_value = SimpleNamespace(
        getMap=lambda useLiteMode: {
            scheme: SimpleNamespace(definition=f"{path}.{'lite' if useLiteMode else 'native'}")
            for scheme, path in (definitions or {}).items()
        }
    )
    return service


class TestWorkspaceName:
    @pytest.mark.parametrize("useLiteMode", [False, True])
    def test_name(self, useLiteMode):
        resolution = "lite" if useLiteMode else "native"
        assert (
            groupingWorkspaceName("Column", "12345", useLiteMode)
            == f"__{Config['grouping.workspacename.' + resolution]}_Column_12345"
        )

    @pytest.mark.parametrize("useLiteMode", [False, True])
    def test_lite_scheme(self, useLiteMode):
        assert groupingWorkspaceName("Lite", "12345", useLiteMode) == wng.liteDataMap().build()


class TestGroupingFileSource:
    def source(self, resolution=Resolution.NATIVE, definitions=None, grocer=None, donors=None):
        return GroupingFileSource(resolution, dataService(definitions), grocer or mock.Mock(), donors or mock.Mock())

    def test_kind(self):
        s = self.source()
        assert s.kind == DataSourceKind.GROUPING_FILE
        assert s.product == Product.GROUPING
        assert s.available(item())
        assert not s.exclusiveFor(item())

    def test_outputName(self):
        assert self.source().outputName(item()) == groupingWorkspaceName("Column", "12345", False)

    @pytest.mark.parametrize("useLiteMode", [False, True])
    def test_filename(self, useLiteMode):
        s = self.source(definitions={"Column": "/column"})
        assert s.filename(item(useLiteMode=useLiteMode)) == f"/column.{'lite' if useLiteMode else 'native'}"
        s.dataService.readGroupingMap.assert_called_once_with("12345")

    @pytest.mark.parametrize("useLiteMode", [False, True])
    def test_filename_lite_scheme(self, useLiteMode):
        # The lite-mode map is not in the grouping map.
        s = self.source()
        assert s.filename(item("Lite", useLiteMode=useLiteMode)) == str(Config["instrument.lite.map.file"])
        s.dataService.readGroupingMap.assert_not_called()

    def test_filename_unknown_scheme(self):
        with pytest.raises(KeyError):
            self.source(definitions={"Column": "/column"}).filename(item("Bank"))

    def test_load_with_an_instrument_donor(self):
        grocer = mock.Mock()
        grocer.executeRecipe.return_value = {"result": True, "loader": LOADER, "workspace": "ws"}
        donors = mock.Mock()
        donors.fetch.return_value = "donor"
        s = self.source(definitions={"Column": "/column"}, grocer=grocer, donors=donors)
        data = s.load(item(), "ws")
        donors.fetch.assert_called_once_with("12345", False)
        grocer.executeRecipe.assert_called_once_with(
            filename="/column.native",
            workspace="ws",
            loader=LOADER,
            instrumentPropertySource="InstrumentDonor",
            instrumentSource="donor",
        )
        assert (data.workspace, data.loader, data.provenance) == ("ws", LOADER, [DataSourceKind.GROUPING_FILE])

    def test_load_with_the_item_instrument(self):
        grocer = mock.Mock()
        grocer.executeRecipe.return_value = {"result": True, "loader": LOADER, "workspace": "ws"}
        donors = mock.Mock()
        s = self.source(definitions={"Column": "/column"}, grocer=grocer, donors=donors)
        s.load(item(instrumentPropertySource="InstrumentFilename", instrumentSource="/instrument.xml"), "ws")
        donors.fetch.assert_not_called()
        kwargs = grocer.executeRecipe.call_args.kwargs
        assert (kwargs["instrumentPropertySource"], kwargs["instrumentSource"]) == (
            "InstrumentFilename",
            "/instrument.xml",
        )


class TestGroupingDataCache:
    def cache(self, *resident):
        return GroupingDataCache(dataService(), FakeADS(*resident))

    def test_key_is_the_state(self):
        c = self.cache()
        assert c.key(item(runNumber="1")) == ("Column", STATE, False)
        assert c.key(item(runNumber="1")) == c.key(item(runNumber="2"))
        assert c.key(item(useLiteMode=True)) != c.key(item(useLiteMode=False))
        assert c.key(item("Bank")) != c.key(item("Column"))

    def test_not_cached(self):
        c = self.cache()
        assert c.lookup(item(), "ws") == ("ws", False)
        assert c.entries == {}

    def test_recorded_and_resident(self):
        c = self.cache("ws")
        c.record(item(), "ws")
        assert c.lookup(item(), "ws") == ("ws", True)
        assert c.cachedWorkspaces() == ["ws"]

    def test_another_run_of_the_same_state(self):
        c = self.cache("ws_first_run")
        c.record(item(runNumber="1"), "ws_first_run")
        assert c.lookup(item(runNumber="2"), "ws_second_run") == ("ws_first_run", True)

    def test_recorded_but_not_resident(self):
        # The cached name is kept, and the entry is dropped: the workspace is reloaded under that name.
        c = self.cache()
        c.record(item(runNumber="1"), "ws_first_run")
        assert c.lookup(item(runNumber="2"), "ws_second_run") == ("ws_first_run", False)
        assert c.entries == {}

    def test_resident_but_not_recorded(self):
        c = self.cache("ws")
        assert c.lookup(item(), "ws") == ("ws", True)
        assert c.entries == {("Column", STATE, False): "ws"}

    def test_rebuild(self):
        c = self.cache("ws_resident")
        c.record(item("Column"), "ws_resident")
        c.record(item("Bank"), "ws_deleted")
        c.rebuild()
        assert c.entries == {("Column", STATE, False): "ws_resident"}
        assert c.cachedWorkspaces() == ["ws_resident"]
