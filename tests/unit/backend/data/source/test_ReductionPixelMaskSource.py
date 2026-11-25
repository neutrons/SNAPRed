import json
from pathlib import Path
from unittest import mock

import pytest

from snapred.backend.dao.ingredients.GroceryListItem import GroceryListItem
from snapred.backend.data.source.CalibrationSources import diffCalTableExtension
from snapred.backend.data.source.DataSourceKind import DataSourceKind
from snapred.backend.data.source.Product import Product
from snapred.backend.data.source.ReductionPixelMaskSource import (
    LOADER,
    ReductionPixelMaskSource,
    reductionPixelMaskName,
)
from snapred.backend.data.source.Resolution import Resolution
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceNameGenerator as wng

TIMESTAMP = 1700000000.0


def item(useLiteMode=False, **kwargs) -> GroceryListItem:
    return GroceryListItem(
        workspaceType="reduction_pixel_mask", runNumber="12345", useLiteMode=useLiteMode, timestamp=TIMESTAMP, **kwargs
    )


def source(grocer=None, donors=None) -> ReductionPixelMaskSource:
    dataService = mock.Mock()
    dataService._constructReductionDataPath.side_effect = lambda runNumber, useLiteMode, timestamp: Path(
        f"/reduction/{runNumber}/{'lite' if useLiteMode else 'native'}/{timestamp}"
    )
    if grocer is None:
        grocer = mock.Mock()
        grocer.executeRecipe.side_effect = lambda **kwargs: {
            "result": True,
            "loader": LOADER,
            "workspace": kwargs["workspace"],
        }
    return ReductionPixelMaskSource(Resolution.NATIVE, dataService, grocer, donors or mock.Mock())


def test_name():
    expected = wng.reductionPixelMask().runNumber("12345").timestamp(TIMESTAMP).build()
    assert reductionPixelMaskName("12345", TIMESTAMP) == expected


def test_kind():
    s = source()
    assert s.kind == DataSourceKind.REDUCTION_RECORD
    assert s.product == Product.REDUCTION_PIXEL_MASK
    assert s.available(item())
    assert not s.exclusiveFor(item())


@pytest.mark.parametrize("useLiteMode", [False, True])
def test_outputName_does_not_depend_on_the_resolution(useLiteMode):
    assert source().outputName(item(useLiteMode)) == reductionPixelMaskName("12345", TIMESTAMP)


@pytest.mark.parametrize("useLiteMode", [False, True])
def test_filename(useLiteMode):
    resolution = "lite" if useLiteMode else "native"
    assert source().filename(item(useLiteMode)) == str(
        Path(f"/reduction/12345/{resolution}/{TIMESTAMP}")
        / (reductionPixelMaskName("12345", TIMESTAMP) + diffCalTableExtension())
    )


def test_load_only_the_mask():
    donors = mock.Mock()
    donors.fetch.return_value = "donor"
    s = source(donors=donors)
    data = s.load(item(), "mask")
    donors.fetch.assert_called_once_with("12345", False)
    s.grocer.executeRecipe.assert_called_once_with(
        filename=s.filename(item()),
        workspace="mask",
        loader=LOADER,
        instrumentPropertySource="InstrumentDonor",
        instrumentSource="donor",
        loaderArgs=json.dumps({"MaskWorkspace": "mask"}),
    )
    assert (data.workspace, data.loader, data.provenance) == ("mask", LOADER, [DataSourceKind.REDUCTION_RECORD])


def test_load_with_the_item_instrument():
    donors = mock.Mock()
    s = source(donors=donors)
    s.load(item(instrumentPropertySource="InstrumentFilename", instrumentSource="/instrument.xml"), "mask")
    donors.fetch.assert_not_called()
    kwargs = s.grocer.executeRecipe.call_args.kwargs
    assert (kwargs["instrumentPropertySource"], kwargs["instrumentSource"]) == ("InstrumentFilename", "/instrument.xml")
