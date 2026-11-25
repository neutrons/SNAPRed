from pathlib import Path
from unittest import mock

import pytest

from snapred.backend.dao.ingredients.GroceryListItem import GroceryListItem
from snapred.backend.data.source.DataSourceKind import DataSourceKind
from snapred.backend.data.source.MonitorSource import LOADER, MonitorSource, monitorName
from snapred.backend.data.source.Product import Product
from snapred.backend.data.source.Resolution import Resolution
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceNameGenerator as wng


def item(useLiteMode=False) -> GroceryListItem:
    return GroceryListItem(workspaceType="neutron", runNumber="12345", useLiteMode=useLiteMode)


def source(filePath="/IPTS-1/nexus/SNAP_12345.nxs.h5") -> MonitorSource:
    dataService = mock.Mock()
    dataService.createNeutronFilePath.return_value = None if filePath is None else Path(filePath)
    grocer = mock.Mock()
    grocer.executeRecipe.return_value = {"result": True, "loader": LOADER, "workspace": "ws"}
    return MonitorSource(dataService, grocer)


def test_name():
    assert monitorName("12345") == wng.monitor().runNumber("12345").build()


def test_kind():
    s = source()
    assert s.kind == DataSourceKind.NEXUS_FILE
    assert s.product == Product.MONITOR
    assert s.provides == Resolution.NATIVE
    assert s.available(item())
    assert not s.exclusiveFor(item())


@pytest.mark.parametrize("useLiteMode", [False, True])
def test_outputName(useLiteMode):
    assert source().outputName(item(useLiteMode)) == monitorName("12345")


def test_load_from_the_native_file():
    s = source()
    data = s.load(item(), "ws")
    s.dataService.createNeutronFilePath.assert_called_once_with("12345", False)
    s.grocer.executeRecipe.assert_called_once_with("/IPTS-1/nexus/SNAP_12345.nxs.h5", "ws", LOADER)
    assert (data.workspace, data.loader, data.provenance) == ("ws", LOADER, [DataSourceKind.NEXUS_FILE])


def test_no_ipts_directory():
    s = source(filePath=None)
    # `ReductionService` matches this message.
    with pytest.raises(RuntimeError, match=r"Cannot find IPTS directory for run '12345'"):
        s.load(item(), "ws")
    s.grocer.executeRecipe.assert_not_called()
