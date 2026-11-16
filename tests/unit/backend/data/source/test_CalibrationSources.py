import json
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pytest
from mantid.simpleapi import DeleteWorkspace, mtd
from util.Config_helpers import Config_override
from util.helpers import createCompatibleDiffCalTable

from snapred.backend.dao.indexing.Versioning import VERSION_START, VersionState
from snapred.backend.dao.ingredients.GroceryListItem import GroceryListItem
from snapred.backend.data.source.CalibrationSources import (
    CalibrationFileSource,
    CalibrationRecordSource,
    diffCalTableExtension,
    diffCalTableName,
    validateCalibrationTable,
)
from snapred.backend.data.source.DataSourceKind import DataSourceKind
from snapred.backend.data.source.Product import Product
from snapred.backend.data.source.Resolution import Resolution
from snapred.backend.data.source.WorkspaceOperations import WorkspaceOperations
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceNameGenerator as wng
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceType

RUN = "12345"
DIFFCAL_RUN = "12300"
VERSION = 7
TABLE = diffCalTableName(DIFFCAL_RUN, VERSION)
MASK = wng.diffCalMask().runNumber(DIFFCAL_RUN).version(VERSION).build()


def item(**kwargs) -> GroceryListItem:
    args = dict(workspaceType="diffcal_table", runNumber=RUN, useLiteMode=False, state="s", diffCalVersion=VERSION)
    args.update(kwargs)
    return GroceryListItem(**args)


def record(withMask=True, table=TABLE):
    workspaces = {WorkspaceType.DIFFCAL_TABLE: [table]}
    if withMask:
        workspaces[WorkspaceType.DIFFCAL_MASK] = [MASK]
    return SimpleNamespace(runNumber=DIFFCAL_RUN, workspaces=workspaces)


def recordSource(product, rec=None, tmp_path=Path("/calib")):
    indexer = mock.Mock()
    indexer.readRecord.return_value = rec
    indexer.latestApplicableVersion.return_value = VERSION
    indexer.versionPath.return_value = tmp_path
    dataService = mock.Mock()
    dataService.calibrationIndexer.return_value = indexer
    grocer = mock.Mock()
    grocer.executeRecipe.return_value = {"result": True, "loader": "LoadCalibrationWorkspaces"}
    donors = mock.Mock()
    donors.fetch.return_value = "donor"
    source = CalibrationRecordSource(product, Resolution.NATIVE, dataService, grocer, mock.Mock(), donors)
    return source, indexer, grocer


def test_diffCalTableName():
    assert TABLE == wng.diffCalTable().runNumber(DIFFCAL_RUN).version(VERSION).build()
    default = wng.diffCalTable().runNumber("default").version(VersionState.DEFAULT).build()
    assert diffCalTableName(DIFFCAL_RUN, VersionState.DEFAULT) == default
    assert diffCalTableName(DIFFCAL_RUN, VERSION_START()) == default


def test_diffCalTableExtension():
    assert diffCalTableExtension() == ".h5"


def test_only_calibration_products():
    with pytest.raises(RuntimeError, match=r"'normalization' is not a diffraction-calibration product"):
        CalibrationFileSource(Product.NORMALIZATION, Resolution.NATIVE, mock.Mock(), mock.Mock(), mock.Mock())


@pytest.mark.parametrize(("product", "name"), [(Product.DIFFCAL_TABLE, TABLE), (Product.DIFFCAL_MASK, MASK)])
def test_record_outputName(product, name):
    source, indexer, _ = recordSource(product, record())
    assert source.kind == DataSourceKind.CALIBRATION_RECORD
    assert source.product == product
    assert source.available(item())
    assert not source.exclusiveFor(item())
    assert source.outputName(item()) == name
    indexer.readRecord.assert_called_with(VERSION)


def test_record_without_a_mask():
    source, _, _ = recordSource(Product.DIFFCAL_MASK, record(withMask=False))
    assert source.outputName(item()) is None


def test_record_not_found():
    source, _, _ = recordSource(Product.DIFFCAL_TABLE, None)
    with pytest.raises(RuntimeError, match=r"Could not find calibration record for run 12345 and version 7"):
        source.outputName(item())


def test_record_without_a_table():
    rec = SimpleNamespace(runNumber=DIFFCAL_RUN, workspaces={WorkspaceType.DIFFCAL_MASK: [MASK]})
    source, _, _ = recordSource(Product.DIFFCAL_MASK, rec)
    with pytest.raises(RuntimeError, match=r"Could not find any table workspaces in calibration record"):
        source.outputName(item())


def test_record_latest_version():
    source, indexer, _ = recordSource(Product.DIFFCAL_TABLE, record())
    noVersion = item().model_copy(update={"diffCalVersion": None})
    assert source.outputName(noVersion) == TABLE
    indexer.latestApplicableVersion.assert_called_once_with(RUN)


@pytest.mark.parametrize(
    ("product", "workspace", "loaderArgs"),
    [
        (Product.DIFFCAL_TABLE, "", {"CalibrationTable": TABLE, "MaskWorkspace": ""}),
        (Product.DIFFCAL_MASK, MASK, {"CalibrationTable": "", "MaskWorkspace": MASK}),
    ],
)
def test_record_load_requests_its_one_output(product, workspace, loaderArgs):
    source, _, grocer = recordSource(product, record(), Path("/calib/v_0007"))
    output = TABLE if product == Product.DIFFCAL_TABLE else MASK
    with mock.patch("snapred.backend.data.source.CalibrationSources.validateCalibrationTable") as validate:
        data = source.load(item(), output)
    grocer.executeRecipe.assert_called_once_with(
        filename=f"/calib/v_0007/{TABLE}.h5",
        workspace=workspace,
        loader="LoadCalibrationWorkspaces",
        instrumentPropertySource="InstrumentDonor",
        instrumentSource="donor",
        loaderArgs=json.dumps(loaderArgs),
    )
    assert data.workspace == output
    assert data.provenance == [DataSourceKind.CALIBRATION_RECORD]
    assert validate.called == (product == Product.DIFFCAL_TABLE)


def test_record_load_uses_the_item_instrument():
    source, _, grocer = recordSource(Product.DIFFCAL_MASK, record())
    source.load(item(instrumentPropertySource="InstrumentName", instrumentSource="SNAP"), MASK)
    kwargs = grocer.executeRecipe.call_args.kwargs
    assert (kwargs["instrumentPropertySource"], kwargs["instrumentSource"]) == ("InstrumentName", "SNAP")
    source.instrumentDonors.fetch.assert_not_called()


def test_record_load_name_mismatch():
    source, _, _ = recordSource(Product.DIFFCAL_TABLE, record(table="some_other_table"))
    with pytest.raises(ValueError, match=r"does not match the expected diffcal table workspace name"):
        source.load(item(), "some_other_table")


@pytest.mark.parametrize(
    ("product", "name"), [(Product.DIFFCAL_TABLE, "my_cal"), (Product.DIFFCAL_MASK, "my_cal_mask")]
)
def test_file_source(product, name):
    grocer = mock.Mock()
    grocer.executeRecipe.return_value = {"result": True, "loader": "LoadCalibrationWorkspaces"}
    source = CalibrationFileSource(product, Resolution.LITE, grocer, mock.Mock(), mock.Mock())
    assert source.kind == DataSourceKind.CALIBRATION_FILE
    assert not source.available(item())
    assert not source.exclusiveFor(item())
    withFile = item(diffCalFilePath=Path("/some/where/my_cal.h5"))
    assert source.available(withFile)
    assert source.exclusiveFor(withFile)
    assert source.outputName(withFile) == name
    with mock.patch("snapred.backend.data.source.CalibrationSources.validateCalibrationTable"):
        source.load(withFile, name)
    assert grocer.executeRecipe.call_args.kwargs["filename"] == str(Path("/some/where/my_cal.h5").resolve())


@pytest.fixture
def table():
    from mantid.simpleapi import CreateSampleWorkspace, LoadInstrument

    from snapred.meta.Config import Resource

    donor = mtd.unique_hidden_name()
    CreateSampleWorkspace(OutputWorkspace=donor, NumBanks=4, BankPixelWidth=2)
    LoadInstrument(
        Workspace=donor,
        Filename=Resource.getPath("inputs/testInstrument/fakeSNAP_Definition.xml"),
        RewriteSpectraMap=True,
    )
    name = mtd.unique_hidden_name()
    createCompatibleDiffCalTable(name, donor)
    yield name
    for ws in (donor, name):
        if mtd.doesExist(ws):
            DeleteWorkspace(ws)


def workspaces():
    return WorkspaceOperations(MantidSnapper(None, "Utensils"))


def test_validateCalibrationTable(table):
    with Config_override("instrument.native.pixelResolution", 16):
        validateCalibrationTable(workspaces(), False, table)


def test_validateCalibrationTable_rows(table):
    with Config_override("instrument.lite.pixelResolution", 17):
        with pytest.raises(RuntimeError, match=r"Expected 17 rows for the lite resolution"):
            validateCalibrationTable(workspaces(), True, table)


def test_validateCalibrationTable_missing():
    with pytest.raises(RuntimeError, match=r"does not exist in the ADS"):
        validateCalibrationTable(workspaces(), False, "does_not_exist")


def test_validateCalibrationTable_difc(table):
    mtd[table].removeColumn("difc")
    mtd[table].addColumn("float", "difc")
    with Config_override("instrument.native.pixelResolution", 16):
        with pytest.raises(RuntimeError, match=r"All 'difc' values must be positive floats"):
            validateCalibrationTable(workspaces(), False, table)
