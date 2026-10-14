"""
Read failover, end to end: an empty, QA-stamped calibration home, reading the mirror's calibrations as its
  prod read-failover, as an `exp` deployment does.

* The prod calibrations are read, and their workspaces loaded, from the failover.
* Saving a calibration initializes the state in home from the failover, then writes only to home:
  home's calibration then shadows the failover's, while the other mode, and the normalizations, still read prod.
* Nothing is ever written to the failover.

The failover root is a copy of the mirror's calibration home, stamped for prod: the mirror itself is never stamped.
  Its small files (e.g. its indexes and records) are copied, so that even a write that bypasses the write guard
  can't reach the mirror; only its large binary files (e.g. its workspaces) are symlinked.
"""

import os
import shutil
import tempfile
from contextlib import ExitStack
from pathlib import Path

import pytest
from mantid.kernel import amend_config
from mantid.simpleapi import CloneWorkspace, mtd
from util.Config_helpers import Config_override

from snapred.backend.dao.calibration.CalibrationHomeMarker import CalibrationHomeMarker
from snapred.backend.dao.indexing.Versioning import VersionState
from snapred.backend.dao.request import (
    CalibrationExportRequest,
    CalibrationLoadAssessmentRequest,
    CreateCalibrationRecordRequest,
    CreateIndexEntryRequest,
)
from snapred.backend.data.CalibrationRoot import calibrationRoot
from snapred.backend.data.DataFactoryService import DataFactoryService
from snapred.backend.data.GroceryService import GroceryService
from snapred.backend.data.LocalDataService import LocalDataService
from snapred.backend.service.CalibrationService import CalibrationService
from snapred.meta.builder.GroceryListBuilder import GroceryListBuilder
from snapred.meta.Config import Config
from snapred.meta.redantic import write_model_pretty
from snapred.ui.main import prependDataSearchDirectories

RUN = "58882"
STATE = "04bd2c53f6bf6754"
# the mirror's calibrations of this state, in native mode: its default version, and version 1
LITE_MODE = False


def stamp(root: Path, environment: str):
    marker = CalibrationHomeMarker(
        schemaVersion=Config["instrument.calibration.schemaVersion"],
        environment=environment,
        createdBy="test_read_failover",
    )
    write_model_pretty(marker, root / CalibrationHomeMarker.FILENAME)


# larger files are symlinked: they are only ever written by the (guarded) workspace writes
SYMLINK_SIZE = 1024 * 1024


def copyOrLink(src, dst):
    if Path(src).stat().st_size >= SYMLINK_SIZE:
        os.symlink(src, dst)
    else:
        shutil.copy2(src, dst)


def copyTree(mirror: Path, target: Path):
    def ignore(directory, names):
        ignored = [name for name in names if name.startswith(".") or name == CalibrationHomeMarker.FILENAME]
        if Path(directory) == mirror / "Powder":
            # e.g. the temporary state roots of an interrupted `calibration_home_from_mirror`
            ignored += [name for name in names if name.startswith("tmp")]
        return ignored

    shutil.copytree(mirror, target, copy_function=copyOrLink, ignore=ignore, dirs_exist_ok=True)


def snapshot(root: Path):
    # every file in `root`, and its content's identity: including the target of any symlink
    return {
        str(path.relative_to(root)): (path.stat().st_size, path.stat().st_mtime_ns)
        for path in sorted(root.rglob("*"))
        if path.is_file()
    }


def clearIndexers():
    LocalDataService()._calibrationIndexer.cache_clear()
    LocalDataService()._normalizationIndexer.cache_clear()


@pytest.fixture
def failoverHomes():
    mirror = Path(Config["instrument.calibration.home"])
    if not (mirror / "Powder" / STATE).is_dir():
        pytest.skip(f"the mirror's calibration home '{mirror}' has no state '{STATE}'")

    with ExitStack() as stack:
        # as in `src/snapred/__main__.py`
        stack.enter_context(amend_config(data_dir=prependDataSearchDirectories(), prepend_datadir=True))
        # beside the mirror, as for `calibration_home_from_mirror`
        prod = Path(stack.enter_context(tempfile.TemporaryDirectory(dir=mirror.parent, prefix="CalibrationProd_")))
        home = Path(stack.enter_context(tempfile.TemporaryDirectory(dir=mirror.parent, prefix="CalibrationQA_")))
        copyTree(mirror, prod)
        stamp(prod, "prod")
        stamp(home, "qa")

        for key, value in (
            ("instrument.calibration.home", str(home)),
            ("instrument.calibration.environment", "qa"),
            ("instrument.calibration.requireMarker", True),
            ("instrument.calibration.readFailover", {"enabled": True, "paths": {"prod": str(prod)}}),
        ):
            stack.enter_context(Config_override(key, value))
        # for these integration tests `LocalDataService` is a singleton: its indexers belong to the previous root
        clearIndexers()
        stack.callback(clearIndexers)
        yield home, prod, mirror


@pytest.fixture
def cleanWorkspaces():
    before = set(mtd.getObjectNames())
    yield
    for name in set(mtd.getObjectNames()) - before:
        mtd.remove(name)


@pytest.mark.datarepo
@pytest.mark.integration
@pytest.mark.usefixtures("cleanWorkspaces")
def test_read_failover(failoverHomes):
    home, prod, mirror = failoverHomes
    prodBefore = snapshot(prod)
    mirrorBefore = snapshot(mirror / "Powder" / STATE)
    lds = LocalDataService()
    dataFactory = DataFactoryService()

    # the configured root reads the mirror as its prod failover
    root = calibrationRoot()
    assert root.home == home
    assert root.failovers == [prod]
    assert Path(Config["instrument.calibration.powder.home"]) == home / "Powder"

    ## Reads come from the failover ##

    state, _ = dataFactory.constructStateId(RUN)
    assert state == STATE
    assert lds.stateExists(RUN)
    assert dataFactory.calibrationExists(RUN, LITE_MODE, state)
    assert dataFactory.getLatestApplicableCalibrationVersion(RUN, LITE_MODE, state) == 1
    assert dataFactory.getLatestApplicableNormalizationVersion(RUN, LITE_MODE, state) == 1
    assert lds.calibrationIndexer(LITE_MODE, state).store.isFailover
    assert lds.readGroupingMap(RUN).stateId.hex == state
    prodRecord = lds.readCalibrationRecord(RUN, LITE_MODE, state, 1)
    assert prodRecord.version == 1

    # the calibration's workspaces are loaded from the failover
    CalibrationService().loadQualityAssessment(
        CalibrationLoadAssessmentRequest(runId=RUN, useLiteMode=LITE_MODE, version=1, checkExistent=False)
    )
    for names in prodRecord.workspaces.values():
        for name in names:
            assert mtd.doesExist(name), name
    # and the normalization's
    normalization = GroceryService().fetchGroceryList(
        GroceryListBuilder().normalization(RUN, state, 1).useLiteMode(LITE_MODE).buildList()
    )
    assert mtd.doesExist(normalization[0])

    # reading wrote nothing to home
    assert sorted(path.name for path in home.iterdir()) == [CalibrationHomeMarker.FILENAME]

    ## Saving a calibration writes it to home ##

    # as if from a new diffraction calibration: the save renames these to their saved names
    workspaces = {}
    for workspaceType, names in prodRecord.workspaces.items():
        workspaces[workspaceType] = []
        for name in names:
            clone = f"new_{name}"
            CloneWorkspace(InputWorkspace=name, OutputWorkspace=clone)
            workspaces[workspaceType].append(clone)
    entry = CreateIndexEntryRequest(
        runNumber=RUN,
        useLiteMode=LITE_MODE,
        version=VersionState.NEXT,
        appliesTo=f">={RUN}",
        comments="failover",
        author="test",
    )
    request = CreateCalibrationRecordRequest(
        runNumber=RUN,
        useLiteMode=LITE_MODE,
        version=VersionState.NEXT,
        calculationParameters=prodRecord.calculationParameters,
        crystalInfo=prodRecord.crystalInfo,
        pixelGroups=prodRecord.pixelGroups,
        focusGroupCalibrationMetrics=prodRecord.focusGroupCalibrationMetrics,
        workspaces=workspaces,
        indexEntry=entry,
    )
    CalibrationService().save(CalibrationExportRequest(createRecordRequest=request))

    # nothing was written to the failover
    assert snapshot(prod) == prodBefore

    # the state was initialized in home from the failover: its grouping map, and its default calibration in each mode
    stateRoot = home / "Powder" / STATE
    assert (stateRoot / "groupingMap.json").read_bytes() == (prod / "Powder" / STATE / "groupingMap.json").read_bytes()
    nativeHome = stateRoot / "native" / "diffraction"
    liteHome = stateRoot / "lite" / "diffraction"
    assert sorted(entry.version for entry in lds.calibrationIndexer(LITE_MODE, state).getIndex()) == [1]
    assert (nativeHome / "v_0000").is_dir()
    assert (liteHome / "v_0000").is_dir()
    assert not (liteHome / "v_0001").exists()

    # the new calibration is home's version 1: it now shadows the failover's
    savedRecord = lds.readCalibrationRecord(RUN, LITE_MODE, state, 1)
    assert savedRecord.indexEntry.comments == "failover"
    assert not lds.calibrationIndexer(LITE_MODE, state).store.isFailover
    assert lds.calibrationIndexer(LITE_MODE, state).rootDirectory == nativeHome
    # the same files as the failover's version 1: the diffcal mask is saved with its table
    assert sorted(path.name for path in (nativeHome / "v_0001").iterdir()) == sorted(
        path.name for path in (prod / "Powder" / STATE / "native" / "diffraction" / "v_0001").iterdir()
    )
    assert dataFactory.getNextCalibrationVersion(LITE_MODE, state) == 2

    # the other mode only has its default version in home: it still reads the failover
    liteIndexer = lds.calibrationIndexer(not LITE_MODE, state)
    assert liteIndexer.store.isFailover
    assert liteIndexer.currentVersion() == 1

    # the normalizations are still read from the failover
    normalizationIndexer = lds.normalizationIndexer(LITE_MODE, state)
    assert normalizationIndexer.store.isFailover
    assert dataFactory.getLatestApplicableNormalizationVersion(RUN, LITE_MODE, state) == 1
    assert not (stateRoot / "native" / "normalization").exists()

    # the workspaces saved to home can be loaded back from home
    for name in [name for names in savedRecord.workspaces.values() for name in names]:
        if mtd.doesExist(name):
            mtd.remove(name)
    CalibrationService().loadQualityAssessment(
        CalibrationLoadAssessmentRequest(runId=RUN, useLiteMode=LITE_MODE, version=1, checkExistent=False)
    )
    for names in savedRecord.workspaces.values():
        for name in names:
            assert mtd.doesExist(name), name

    assert snapshot(prod) == prodBefore
    assert snapshot(mirror / "Powder" / STATE) == mirrorBefore


@pytest.mark.datarepo
@pytest.mark.integration
def test_read_failover_disabled(failoverHomes):
    # without the failover, the empty home has no calibrations
    home, _, _ = failoverHomes
    with Config_override("instrument.calibration.readFailover", {"enabled": False}):
        clearIndexers()
        assert calibrationRoot().failovers == []
        assert not LocalDataService().stateExists(RUN)
        assert DataFactoryService().getLatestApplicableCalibrationVersion(RUN, LITE_MODE, STATE) is None
    assert sorted(path.name for path in home.iterdir()) == [CalibrationHomeMarker.FILENAME]
