"""
`IndexStore`: where an indexed directory is read from, where it is written to, and what is done before its first write.
"""

import importlib
import json
from pathlib import Path
from unittest import mock

import pytest

from snapred.backend.dao.calibration.CalibrationHomeMarker import CalibrationHomeMarker
from snapred.backend.data.CalibrationRoot import CalibrationRoot
from snapred.backend.data.IndexerType import IndexerType
from snapred.backend.data.IndexStore import FailoverIndexStore, IndexStore, InstrumentParameterStore, StateIndexStore
from snapred.backend.error.CalibrationWriteGuardError import CalibrationWriteGuardError
from snapred.meta.redantic import write_model_pretty

IndexStoreModule = importlib.import_module(IndexStore.__module__)

SCHEMA = 1
STATE = Path("Powder/0123456789abcdef/lite/diffraction")
INDEX = "CalibrationIndex.json"


def stamp(root: Path, environment: str) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    marker = CalibrationHomeMarker(schemaVersion=SCHEMA, environment=environment, createdBy="test")
    write_model_pretty(marker, root / CalibrationHomeMarker.FILENAME)
    return root


def touch(path: Path, text: str = "") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
    return path


def snapshot(root: Path):
    return {str(p.relative_to(root)): p.read_bytes() for p in sorted(root.rglob("*")) if p.is_file()}


@pytest.fixture
def roots(tmp_path):
    # a QA home, with prod and next failovers (in that priority order)
    home = stamp(tmp_path / "CalibrationQA", "qa")
    prod = stamp(tmp_path / "Calibration", "prod")
    next_ = stamp(tmp_path / "CalibrationNext", "next")
    return home, prod, next_


@pytest.fixture
def calibrationRoot(roots):
    home, prod, next_ = roots
    return CalibrationRoot(home, [prod, next_], environment="qa", schemaVersion=SCHEMA)


def store(calibrationRoot, rel=STATE, **kwargs):
    return FailoverIndexStore(IndexerType.CALIBRATION, calibrationRoot, rel, **kwargs)


##
## A single directory
##


def test_IndexStore(tmp_path):
    plain = IndexStore(IndexerType.NORMALIZATION, tmp_path)
    assert plain.indexerType == IndexerType.NORMALIZATION
    assert plain.indexFilename == "NormalizationIndex.json"
    assert plain.readDirectory == plain.writeDirectory == plain.prepareWrite() == tmp_path
    assert not plain.isFailover
    assert not plain.recoveryMode
    assert IndexStore(IndexerType.NORMALIZATION, str(tmp_path), recoveryMode=True).recoveryMode


##
## A directory within a calibration root
##


def test_FailoverIndexStore(roots, calibrationRoot):
    home, _, _ = roots
    failoverStore = store(calibrationRoot)
    assert failoverStore.root is calibrationRoot
    assert failoverStore.rel == STATE
    assert failoverStore.homeDirectory == home / STATE
    assert failoverStore.indexFilename == INDEX


@pytest.mark.parametrize("rel", ["/abs", "../a", "Powder/../../a"])
def test_rel_must_be_relative(calibrationRoot, rel):
    with pytest.raises(ValueError, match="relative"):
        store(calibrationRoot, rel)


def test_no_index_anywhere(roots, calibrationRoot):
    home, _, _ = roots
    failoverStore = store(calibrationRoot)
    assert failoverStore.readDirectory == home / STATE
    assert failoverStore.writeDirectory == home / STATE
    assert not failoverStore.isFailover


def test_reads_from_failover(roots, calibrationRoot):
    home, prod, next_ = roots
    touch(prod / STATE / INDEX)
    touch(next_ / STATE / INDEX)
    failoverStore = store(calibrationRoot)
    assert failoverStore.readDirectory == prod / STATE
    assert failoverStore.isFailover
    # writes still go to home
    assert failoverStore.writeDirectory == home / STATE


def test_home_directory_without_index_does_not_shadow(roots, calibrationRoot):
    # e.g. a stub state directory in home: the failover's index still wins
    home, prod, _ = roots
    touch(home / STATE / "v_0001" / "partial.json")
    touch(prod / STATE / INDEX)
    assert store(calibrationRoot).readDirectory == prod / STATE


def test_repins_to_home(roots, calibrationRoot):
    home, prod, _ = roots
    touch(prod / STATE / INDEX)
    failoverStore = store(calibrationRoot)
    assert failoverStore.readDirectory == prod / STATE

    # the first write of the index into home shadows the failover
    touch(home / STATE / INDEX)
    assert failoverStore.readDirectory == home / STATE
    assert not failoverStore.isFailover

    # once in home, reads stay in home
    (home / STATE / INDEX).unlink()
    assert failoverStore.readDirectory == home / STATE


def test_home_index(roots, calibrationRoot):
    home, prod, _ = roots
    touch(home / STATE / INDEX)
    touch(prod / STATE / INDEX)
    assert store(calibrationRoot).readDirectory == home / STATE


def test_writes_are_guarded(roots, calibrationRoot):
    home, prod, _ = roots
    (home / "Powder").mkdir()
    (home / "Powder" / "0123456789abcdef").symlink_to(prod)
    failoverStore = store(calibrationRoot)
    with mock.patch.object(failoverStore, "initializeHome") as initializeHome:
        with pytest.raises(CalibrationWriteGuardError):
            failoverStore.writeDirectory
        with pytest.raises(CalibrationWriteGuardError):
            failoverStore.prepareWrite()
        # nothing is initialized behind the guard
        initializeHome.assert_not_called()


def test_prepareWrite_initializes_home_only_without_index(roots, calibrationRoot):
    home, _, _ = roots
    failoverStore = store(calibrationRoot)
    with mock.patch.object(failoverStore, "initializeHome") as initializeHome:
        assert failoverStore.prepareWrite() == home / STATE
        initializeHome.assert_called_once()
        touch(home / STATE / INDEX)
        assert failoverStore.prepareWrite() == home / STATE
        initializeHome.assert_called_once()


def test_initializeHome_default_does_nothing(roots, calibrationRoot):
    home, prod, _ = roots
    touch(prod / STATE / INDEX)
    homeBefore = snapshot(home)
    store(calibrationRoot).prepareWrite()
    assert snapshot(home) == homeBefore


##
## Populated directories
##


class Populated(FailoverIndexStore):
    def __init__(self, *args, isPopulated, **kwargs):
        super().__init__(IndexerType.CALIBRATION, *args, **kwargs)
        self._isPopulated = isPopulated

    def isPopulated(self, directory):
        return self._isPopulated(directory)


def test_isPopulated(roots, calibrationRoot):
    home, prod, next_ = roots
    touch(home / STATE / INDEX, "home")
    touch(prod / STATE / INDEX, "")
    touch(next_ / STATE / INDEX, "next")
    calls = []

    def isPopulated(directory):
        calls.append(directory)
        return (directory / INDEX).read_text() != "" and directory != home / STATE

    # home and prod are not populated: the first populated root wins
    assert Populated(calibrationRoot, STATE, isPopulated=isPopulated).readDirectory == next_ / STATE
    assert calls == [home / STATE, prod / STATE, next_ / STATE]


def test_isPopulated_none_populated(roots, calibrationRoot):
    # nothing is populated: the first root containing the index, home first
    home, prod, _ = roots
    touch(prod / STATE / INDEX)
    failoverStore = Populated(calibrationRoot, STATE, isPopulated=lambda directory: False)  # noqa: ARG005
    assert failoverStore.readDirectory == prod / STATE
    touch(home / STATE / INDEX)
    assert failoverStore.readDirectory == home / STATE
    assert not failoverStore.isFailover


def test_isPopulated_only_called_with_index(roots, calibrationRoot):
    home, prod, _ = roots
    (home / STATE).mkdir(parents=True)
    touch(prod / STATE / INDEX)
    calls = []
    failoverStore = Populated(calibrationRoot, STATE, isPopulated=lambda directory: calls.append(directory) or True)
    assert failoverStore.readDirectory == prod / STATE
    assert calls == [prod / STATE]


def test_isPopulated_pins_home(roots, calibrationRoot):
    home, prod, _ = roots
    touch(home / STATE / INDEX)
    touch(prod / STATE / INDEX)
    populated = {"home": False}
    failoverStore = Populated(
        calibrationRoot, STATE, isPopulated=lambda directory: directory != home / STATE or populated["home"]
    )
    assert failoverStore.readDirectory == prod / STATE
    populated["home"] = True
    assert failoverStore.readDirectory == home / STATE
    populated["home"] = False
    # once populated, home stays pinned
    assert failoverStore.readDirectory == home / STATE


def test_isPopulated_errors_are_not_masked(roots, calibrationRoot):
    home, prod, _ = roots
    touch(home / STATE / INDEX)
    touch(prod / STATE / INDEX)

    def isPopulated(directory):
        raise RuntimeError(f"corrupt: {directory}")

    with pytest.raises(RuntimeError, match="corrupt"):
        Populated(calibrationRoot, STATE, isPopulated=isPopulated).readDirectory


##
## The calibrations and normalizations of a state
##

STATE_ROOT = Path("Powder/0123456789abcdef")
LITE = STATE_ROOT / "lite/diffraction"
NATIVE = STATE_ROOT / "native/diffraction"
NORMALIZATION = STATE_ROOT / "lite/normalization"


def entry(version: int) -> dict:
    return {"runNumber": "12345", "useLiteMode": True, "version": version, "appliesTo": ">=12345"}


def writeIndex(directory: Path, versions, indexName: str = INDEX):
    for version in versions:
        touch(directory / f"v_{version:04d}" / "Record.json", f"v{version}")
    touch(directory / indexName, json.dumps([entry(version) for version in versions]))


def stateStore(calibrationRoot, indexerType=IndexerType.CALIBRATION, rel=LITE):
    return StateIndexStore(indexerType, calibrationRoot, rel, stateRel=STATE_ROOT, calibrationRels=[LITE, NATIVE])


def test_StateIndexStore_calibration_ignores_default_version(roots, calibrationRoot):
    home, prod, _ = roots
    writeIndex(home / LITE, [0])
    writeIndex(prod / LITE, [0, 1])
    calibrationStore = stateStore(calibrationRoot)
    assert calibrationStore.ignoresDefaultVersion
    assert calibrationStore.readDirectory == prod / LITE
    writeIndex(home / LITE, [0, 1])
    assert calibrationStore.readDirectory == home / LITE


def test_StateIndexStore_normalization_version_0_is_real(roots, calibrationRoot):
    home, prod, _ = roots
    writeIndex(home / NORMALIZATION, [0], "NormalizationIndex.json")
    writeIndex(prod / NORMALIZATION, [0, 1], "NormalizationIndex.json")
    normalizationStore = stateStore(calibrationRoot, IndexerType.NORMALIZATION, NORMALIZATION)
    assert not normalizationStore.ignoresDefaultVersion
    assert normalizationStore.readDirectory == home / NORMALIZATION


def test_StateIndexStore_initializeHome(roots, calibrationRoot):
    home, prod, _ = roots
    touch(prod / STATE_ROOT / "groupingMap.json", "prod map")
    writeIndex(prod / LITE, [0, 1, 2])
    writeIndex(prod / NATIVE, [0, 3])
    prodBefore = snapshot(prod)

    assert stateStore(calibrationRoot).prepareWrite() == home / LITE

    assert snapshot(prod) == prodBefore
    assert (home / STATE_ROOT / "groupingMap.json").read_text() == "prod map"
    for rel in (LITE, NATIVE):
        # only the default version, and an index containing only its entry
        assert sorted(path.name for path in (home / rel).iterdir()) == [INDEX, "v_0000"]
        assert [e["version"] for e in json.loads((home / rel / INDEX).read_text())] == [0]
        assert (home / rel / "v_0000" / "Record.json").read_text() == "v0"


def test_StateIndexStore_initializeHome_from_a_normalization_write(roots, calibrationRoot):
    # a normalization's first write also initializes the state's calibrations
    home, prod, _ = roots
    writeIndex(prod / LITE, [0, 1])
    stateStore(calibrationRoot, IndexerType.NORMALIZATION, NORMALIZATION).prepareWrite()
    assert (home / LITE / INDEX).exists()
    assert not (home / NORMALIZATION).exists()


def test_StateIndexStore_initializeHome_keeps_home(roots, calibrationRoot):
    home, prod, _ = roots
    touch(home / STATE_ROOT / "groupingMap.json", "home map")
    writeIndex(home / NATIVE, [0, 5])
    touch(prod / STATE_ROOT / "groupingMap.json", "prod map")
    writeIndex(prod / LITE, [0, 1])
    writeIndex(prod / NATIVE, [0, 3])

    stateStore(calibrationRoot).prepareWrite()

    assert (home / STATE_ROOT / "groupingMap.json").read_text() == "home map"
    assert [e["version"] for e in json.loads((home / NATIVE / INDEX).read_text())] == [0, 5]
    assert [e["version"] for e in json.loads((home / LITE / INDEX).read_text())] == [0]


def test_StateIndexStore_initializeHome_new_state(roots, calibrationRoot):
    # a state in no read-failover: initialized as usual, by `initializeState`
    home, _, _ = roots
    homeBefore = snapshot(home)
    stateStore(calibrationRoot).prepareWrite()
    assert snapshot(home) == homeBefore


def test_StateIndexStore_initializeHome_partial_failover_state(roots, calibrationRoot):
    # the failover has no grouping map, and no default version in one of the modes
    home, prod, _ = roots
    writeIndex(prod / LITE, [1])
    writeIndex(prod / NATIVE, [0])
    stateStore(calibrationRoot).prepareWrite()
    assert not (home / STATE_ROOT / "groupingMap.json").exists()
    assert not (home / LITE).exists()
    assert (home / NATIVE / INDEX).exists()


def test_StateIndexStore_initializeHome_no_temporary_index(roots, calibrationRoot):
    home, prod, _ = roots
    writeIndex(prod / LITE, [0])
    stateStore(calibrationRoot).prepareWrite()
    assert not list(home.rglob("*.tmp"))


##
## The instrument parameters
##

PARAMETERS = Path("SNAPInstPrm")
PARAMETERS_INDEX = "InstrumentParameterIndex.json"


def test_InstrumentParameterStore(calibrationRoot):
    parameterStore = InstrumentParameterStore(calibrationRoot, PARAMETERS)
    assert parameterStore.indexerType == IndexerType.INSTRUMENT_PARAMETER
    assert not parameterStore.ignoresDefaultVersion


def test_InstrumentParameterStore_initializeHome(roots, calibrationRoot):
    home, prod, _ = roots
    writeIndex(prod / PARAMETERS, [0, 1, 2], PARAMETERS_INDEX)
    touch(prod / PARAMETERS / ".hidden")
    prodBefore = snapshot(prod)

    assert InstrumentParameterStore(calibrationRoot, PARAMETERS).prepareWrite() == home / PARAMETERS

    assert snapshot(prod) == prodBefore
    # every epoch is copied, and the index last
    assert sorted(path.name for path in (home / PARAMETERS).iterdir()) == [
        PARAMETERS_INDEX,
        "v_0000",
        "v_0001",
        "v_0002",
    ]
    assert [e["version"] for e in json.loads((home / PARAMETERS / PARAMETERS_INDEX).read_text())] == [0, 1, 2]


def test_InstrumentParameterStore_initializeHome_nothing_in_failover(roots, calibrationRoot):
    home, prod, _ = roots
    # a failover directory without an index is not copied
    touch(prod / PARAMETERS / "v_0000" / "SNAPInstPrm.json")
    InstrumentParameterStore(calibrationRoot, PARAMETERS).prepareWrite()
    assert not (home / PARAMETERS).exists()


def test_InstrumentParameterStore_initializeHome_index_written_last(roots, calibrationRoot):
    # if copying is interrupted, home has no index: it isn't initialized, and is initialized again at the next write
    home, prod, _ = roots
    writeIndex(prod / PARAMETERS, [0, 1], PARAMETERS_INDEX)
    copytree = IndexStoreModule.shutil.copytree

    def interrupted(*args, **kwargs):
        copytree(*args, **kwargs)
        raise OSError("interrupted")

    with mock.patch.object(IndexStoreModule.shutil, "copytree", side_effect=interrupted):
        with pytest.raises(OSError, match="interrupted"):
            InstrumentParameterStore(calibrationRoot, PARAMETERS).prepareWrite()
    assert (home / PARAMETERS / "v_0001").is_dir()
    assert not (home / PARAMETERS / PARAMETERS_INDEX).exists()

    InstrumentParameterStore(calibrationRoot, PARAMETERS).prepareWrite()
    assert (home / PARAMETERS / PARAMETERS_INDEX).exists()
