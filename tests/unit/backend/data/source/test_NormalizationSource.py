from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pytest

from snapred.backend.dao.ingredients.GroceryListItem import GroceryListItem
from snapred.backend.data.source.DataSourceKind import DataSourceKind
from snapred.backend.data.source.NormalizationSource import NormalizationRecordSource
from snapred.backend.data.source.Product import Product
from snapred.backend.data.source.Resolution import Resolution
from snapred.meta.Config import Config
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceNameGenerator as wng

RUN = "12345"
NORMCAL_RUN = "12301"
VERSION = 5


def item(hidden=False) -> GroceryListItem:
    return GroceryListItem(
        workspaceType="normalization",
        runNumber=RUN,
        useLiteMode=True,
        state="s",
        normCalVersion=VERSION,
        hidden=hidden,
        loader="LoadNexusProcessed",
    )


def source(rec=SimpleNamespace(runNumber=NORMCAL_RUN)):
    indexer = mock.Mock()
    indexer.readRecord.return_value = rec
    indexer.versionPath.return_value = Path("/norm/v_0005")
    dataService = mock.Mock()
    dataService.normalizationIndexer.return_value = indexer
    grocer = mock.Mock()
    grocer.executeRecipe.return_value = {"result": True, "loader": "LoadNexusProcessed"}
    return NormalizationRecordSource(Resolution.LITE, dataService, grocer), dataService, grocer


def name(hidden):
    return wng.rawVanadium().runNumber(NORMCAL_RUN).version(VERSION).hidden(hidden).build()


def test_kind():
    src, _, _ = source()
    assert src.kind == DataSourceKind.NORMALIZATION_RECORD
    assert src.product == Product.NORMALIZATION
    assert src.provides == Resolution.LITE
    assert src.available(item())


@pytest.mark.parametrize("hidden", [False, True])
def test_outputName(hidden):
    src, dataService, _ = source()
    assert src.outputName(item(hidden)) == name(hidden)
    dataService.normalizationIndexer.assert_called_with(True, "s")


def test_no_record():
    src, _, _ = source(rec=None)
    with pytest.raises(RuntimeError, match=r"Could not find normalization record for run 12345 and version 5"):
        src.outputName(item())


@pytest.mark.parametrize("hidden", [False, True])
def test_load(hidden):
    # The file is named for the workspace, as it is when it is not hidden.
    src, _, grocer = source()
    data = src.load(item(hidden), name(hidden))
    expected = Path("/norm/v_0005") / (name(False) + Config["calibration.normalization.output.ws.extension"])
    grocer.executeRecipe.assert_called_once_with(
        filename=str(expected), workspace=name(hidden), loader="LoadNexusProcessed"
    )
    assert data.workspace == name(hidden)
    assert data.provenance == [DataSourceKind.NORMALIZATION_RECORD]
