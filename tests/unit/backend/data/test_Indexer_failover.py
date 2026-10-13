"""
`Indexer` over a `FailoverIndexStore`: reads from a read-failover root, writes only to the calibration home.
"""

import importlib
from datetime import datetime
from pathlib import Path
from unittest import mock

import pytest
from util.dao import DAOFactory

from snapred.backend.dao.calibration.CalibrationHomeMarker import CalibrationHomeMarker
from snapred.backend.dao.indexing.CalculationParameters import CalculationParameters
from snapred.backend.dao.indexing.IndexEntry import IndexEntry
from snapred.backend.dao.indexing.Record import Record
from snapred.backend.dao.indexing.Versioning import VERSION_START
from snapred.backend.data.CalibrationRoot import CalibrationRoot
from snapred.backend.data.Indexer import Indexer, IndexerType
from snapred.backend.data.IndexStore import FailoverIndexStore, IndexStore
from snapred.backend.error.CalibrationWriteGuardError import CalibrationWriteGuardError
from snapred.meta.mantid.WorkspaceNameGenerator import ValueFormatter as wnvf
from snapred.meta.redantic import write_model_pretty

IndexStoreModule = importlib.import_module(IndexStore.__module__)

STATE = Path("Powder/0123456789abcdef/lite/diffraction")
# the index filename of an `IndexerType.DEFAULT` indexer
INDEX = "Index.json"


class DefaultIgnoringStore(FailoverIndexStore):
    # e.g. the calibrations of a state: a directory containing only the default version is not populated
    ignoresDefaultVersion = True


def stamp(root: Path, environment: str) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    marker = CalibrationHomeMarker(schemaVersion=1, environment=environment, createdBy="test")
    write_model_pretty(marker, root / CalibrationHomeMarker.FILENAME)
    return root


def record(version: int, runNumber: str = "12345") -> Record:
    indexEntry = IndexEntry(runNumber=runNumber, useLiteMode=True, version=version, appliesTo=f">={runNumber}")
    parameters = CalculationParameters(
        instrumentState=DAOFactory.default_instrument_state,
        seedRun=int(runNumber),
        useLiteMode=True,
        creationDate=datetime.today().isoformat(),
        name="",
        version=version,
        indexEntry=indexEntry,
    )
    return Record(
        runNumber=runNumber,
        useLiteMode=True,
        version=version,
        calculationParameters=parameters,
        indexEntry=indexEntry,
        snapredVersion="test",
    )


def writeVersions(directory: Path, versions):
    # populate a directory independently of any failover
    indexer = Indexer(IndexStore(IndexerType.DEFAULT, directory))
    for version in versions:
        rec = record(version)
        indexer.writeRecord(rec)
        indexer.writeParameters(rec.calculationParameters)


def snapshot(root: Path):
    return {str(p.relative_to(root)): p.read_bytes() for p in sorted(root.rglob("*")) if p.is_file()}


@pytest.fixture
def roots(tmp_path):
    home = stamp(tmp_path / "CalibrationQA", "qa")
    prod = stamp(tmp_path / "Calibration", "prod")
    writeVersions(prod / STATE, [VERSION_START(), 1, 2])
    return home, prod


@pytest.fixture
def calibrationRoot(roots):
    home, prod = roots
    return CalibrationRoot(home, [prod], environment="qa", schemaVersion=1)


@pytest.fixture
def indexer(calibrationRoot):
    return Indexer(FailoverIndexStore(IndexerType.DEFAULT, calibrationRoot, STATE))


def test_directory_indexer(tmp_path):
    indexer = Indexer(IndexStore(IndexerType.DEFAULT, tmp_path))
    assert not indexer.store.isFailover
    assert indexer.store.readDirectory == indexer.store.writeDirectory == tmp_path
    assert indexer.store.prepareWrite() == tmp_path
    assert indexer.rootDirectory == tmp_path


def test_indexerType_is_the_stores(calibrationRoot):
    indexer = Indexer(FailoverIndexStore(IndexerType.CALIBRATION, calibrationRoot, STATE))
    assert indexer.indexerType == IndexerType.CALIBRATION
    assert indexer.indexFilename == "CalibrationIndex.json"


##
## Reads come from the failover
##


def test_reads_from_failover(roots, indexer):
    home, prod = roots
    assert indexer.store.isFailover
    assert indexer.rootDirectory == prod / STATE
    assert sorted(indexer.allVersions()) == [VERSION_START(), 1, 2]
    assert indexer.currentVersion() == 2
    assert indexer.latestApplicableVersion("12345") == 2
    assert indexer.readRecord(2).version == 2
    assert indexer.readParameters(1).version == 1
    assert indexer.versionPath(2) == prod / STATE / wnvf.pathVersion(2)
    assert indexer.indexPath() == prod / STATE / INDEX
    assert [entry.version for entry in indexer.getIndex()] == [1, 2]
    # reading creates nothing in home
    assert not (home / "Powder").exists()


def test_recoverIndex_dryrun_reads_failover(indexer):
    assert sorted(entry.version for entry in indexer.recoverIndex(dryrun=True)) == [VERSION_START(), 1, 2]


##
## Writes go to home
##


def test_nextVersion_uses_home(roots, indexer):
    home, prod = roots
    # home has no versions of this state: the next version there is the first
    assert indexer.nextVersion() == VERSION_START()
    # reads still come from the failover, which is unchanged
    assert indexer.currentVersion() == 2
    assert indexer.rootDirectory == prod / STATE
    assert not (home / "Powder").exists()


def test_writeVersionPath_is_home(roots, indexer):
    home, prod = roots
    assert indexer.writeVersionPath(2) == home / STATE / wnvf.pathVersion(2)
    assert indexer.versionPath(2) == prod / STATE / wnvf.pathVersion(2)


def test_obtainLock_locks_home(roots, indexer):
    home, _ = roots
    lock = indexer.obtainLock()
    try:
        assert lock.lockedPath == (home / STATE).resolve()
    finally:
        lock.release()


def test_first_write_to_home_shadows_failover(roots, indexer):
    home, prod = roots
    prodBefore = snapshot(prod)

    rec = record(VERSION_START(), runNumber="23456")
    indexer.writeRecord(rec)
    indexer.writeParameters(rec.calculationParameters)

    # nothing was written to the failover
    assert snapshot(prod) == prodBefore
    # home has its own index, containing only the new version
    assert (home / STATE / INDEX).exists()
    assert (home / STATE / wnvf.pathVersion(VERSION_START()) / "Record.json").exists()

    # reads now come from home: the failover's versions are no longer visible
    assert not indexer.store.isFailover
    assert indexer.rootDirectory == home / STATE
    assert indexer.allVersions() == [VERSION_START()]
    assert indexer.readRecord(VERSION_START()).runNumber == "23456"
    # never falls through to the failover for a version only it has
    with pytest.raises(FileNotFoundError):
        indexer.readRecord(2)
    # numbering continues in home
    assert indexer.nextVersion() == VERSION_START() + 1


def test_write_numbering_never_mixes_roots(roots, indexer):
    # `VersionState.NEXT` is numbered from home, not from the failover's versions
    _, prod = roots
    rec = record(VERSION_START())
    indexer.writeRecord(rec)
    rec = record(VERSION_START() + 1)
    indexer.writeRecord(rec)
    indexer.writeParameters(rec.calculationParameters)
    assert sorted(indexer.allVersions()) == [VERSION_START(), VERSION_START() + 1]
    assert sorted(Indexer(IndexStore(IndexerType.DEFAULT, prod / STATE)).allVersions()) == [
        VERSION_START(),
        1,
        2,
    ]


def test_recoverIndex_writes_only_home(roots, indexer):
    home, prod = roots
    prodBefore = snapshot(prod)
    # home has no version folders for this state: nothing to recover, nothing written
    assert indexer.recoverIndex(dryrun=False) == []
    assert snapshot(prod) == prodBefore
    assert not (home / STATE / INDEX).exists()


def test_rebinds_to_home_when_another_process_writes(roots, indexer):
    home, _ = roots
    assert indexer.currentVersion() == 2
    # e.g. another process (or another, cached indexer) initializes the state in home
    writeVersions(home / STATE, [VERSION_START()])
    assert indexer.currentVersion() == VERSION_START()
    assert indexer.rootDirectory == home / STATE
    assert not indexer.store.isFailover


def test_write_guard(roots):
    # a home whose state directory is symlinked into prod: reads are allowed, writes are refused
    home, prod = roots
    (home / "Powder").symlink_to(prod / "Powder")
    root = CalibrationRoot(home, [prod], environment="qa", schemaVersion=1)
    indexer = Indexer(FailoverIndexStore(IndexerType.DEFAULT, root, STATE))
    assert indexer.currentVersion() == 2
    prodBefore = snapshot(prod)

    with pytest.raises(CalibrationWriteGuardError):
        indexer.writeRecord(record(3))
    with pytest.raises(CalibrationWriteGuardError):
        indexer.nextVersion()
    with pytest.raises(CalibrationWriteGuardError):
        indexer.writeVersionPath(3)
    with pytest.raises(CalibrationWriteGuardError):
        indexer.obtainLock()
    assert snapshot(prod) == prodBefore


def test_home_only(calibrationRoot, roots):
    # with home containing the index, the failover is never consulted
    home, _ = roots
    writeVersions(home / STATE, [VERSION_START(), 1])
    indexer = Indexer(FailoverIndexStore(IndexerType.DEFAULT, calibrationRoot, STATE))
    assert not indexer.store.isFailover
    assert sorted(indexer.allVersions()) == [VERSION_START(), 1]
    assert indexer.versionPath(1) == indexer.writeVersionPath(1) == home / STATE / wnvf.pathVersion(1)


##
## A home containing only the default version does not shadow the failover
##


@pytest.fixture
def initializedIndexer(calibrationRoot, roots):
    # the state was initialized in home: its index contains only the default version
    home, _ = roots
    writeVersions(home / STATE, [VERSION_START()])
    return Indexer(DefaultIgnoringStore(IndexerType.DEFAULT, calibrationRoot, STATE))


def test_initialized_home_reads_failover(roots, initializedIndexer):
    _, prod = roots
    assert initializedIndexer.store.isFailover
    assert initializedIndexer.rootDirectory == prod / STATE
    assert sorted(initializedIndexer.allVersions()) == [VERSION_START(), 1, 2]
    assert initializedIndexer.latestApplicableVersion("12345") == 2


def test_initialized_home_numbering_continues_in_home(roots, initializedIndexer):
    home, prod = roots
    prodBefore = snapshot(prod)
    # home already has the default version
    assert initializedIndexer.nextVersion() == VERSION_START() + 1
    rec = record(VERSION_START() + 1, runNumber="23456")
    initializedIndexer.writeRecord(rec)
    initializedIndexer.writeParameters(rec.calculationParameters)
    assert snapshot(prod) == prodBefore

    # home is now populated: it shadows the failover, and stays pinned
    assert not initializedIndexer.store.isFailover
    assert initializedIndexer.rootDirectory == home / STATE
    assert sorted(initializedIndexer.allVersions()) == [VERSION_START(), VERSION_START() + 1]
    assert initializedIndexer.readRecord(VERSION_START() + 1).runNumber == "23456"


def test_initialized_home_other_process_initializing_does_not_shadow(roots, calibrationRoot):
    # a cached indexer keeps reading the failover when home is only initialized
    home, prod = roots
    indexer = Indexer(DefaultIgnoringStore(IndexerType.DEFAULT, calibrationRoot, STATE))
    writeVersions(home / STATE, [VERSION_START()])
    assert indexer.currentVersion() == 2
    assert indexer.rootDirectory == prod / STATE
    writeVersions(home / STATE, [VERSION_START() + 1])
    assert indexer.currentVersion() == VERSION_START() + 1
    assert indexer.rootDirectory == home / STATE


def test_default_only_everywhere_reads_home(tmp_path):
    # with no populated root, home's own default version is read
    home = stamp(tmp_path / "CalibrationQA", "qa")
    prod = stamp(tmp_path / "Calibration", "prod")
    writeVersions(home / STATE, [VERSION_START()])
    writeVersions(prod / STATE, [VERSION_START()])
    root = CalibrationRoot(home, [prod], environment="qa", schemaVersion=1)
    indexer = Indexer(DefaultIgnoringStore(IndexerType.DEFAULT, root, STATE))
    assert not indexer.store.isFailover
    assert indexer.rootDirectory == home / STATE


def test_default_only_failover_without_home_index(tmp_path):
    home = stamp(tmp_path / "CalibrationQA", "qa")
    prod = stamp(tmp_path / "Calibration", "prod")
    writeVersions(prod / STATE, [VERSION_START()])
    root = CalibrationRoot(home, [prod], environment="qa", schemaVersion=1)
    indexer = Indexer(DefaultIgnoringStore(IndexerType.DEFAULT, root, STATE))
    assert indexer.rootDirectory == prod / STATE


def test_ignoresDefaultVersion(calibrationRoot, tmp_path):
    store = DefaultIgnoringStore(IndexerType.DEFAULT, calibrationRoot, STATE)
    writeVersions(tmp_path / "default", [VERSION_START()])
    writeVersions(tmp_path / "populated", [VERSION_START(), 1])
    writeVersions(tmp_path / "noDefault", [1])
    assert not store.isPopulated(tmp_path / "default")
    assert store.isPopulated(tmp_path / "populated")
    assert store.isPopulated(tmp_path / "noDefault")
    # by default, any directory containing the index is populated
    assert FailoverIndexStore(IndexerType.DEFAULT, calibrationRoot, STATE).isPopulated(tmp_path / "default")


def test_ignoresDefaultVersion_corrupt_home_index_is_not_masked(roots, calibrationRoot):
    home, _ = roots
    (home / STATE).mkdir(parents=True)
    (home / STATE / INDEX).write_text("{ corrupt")
    with pytest.raises(ValueError, match="Invalid JSON"):
        DefaultIgnoringStore(IndexerType.DEFAULT, calibrationRoot, STATE).readDirectory


def test_ignoresDefaultVersion_cache(calibrationRoot, tmp_path):
    store = DefaultIgnoringStore(IndexerType.DEFAULT, calibrationRoot, STATE)
    writeVersions(tmp_path, [VERSION_START()])
    with mock.patch.object(IndexStoreModule, "parse_file_as", wraps=IndexStoreModule.parse_file_as) as parse:
        assert not store.isPopulated(tmp_path)
        assert not store.isPopulated(tmp_path)
        assert parse.call_count == 1
    # the index changes: it is re-parsed
    writeVersions(tmp_path, [1])
    with mock.patch.object(IndexStoreModule, "parse_file_as", wraps=IndexStoreModule.parse_file_as) as parse:
        assert store.isPopulated(tmp_path)
        assert parse.call_count == 1


##
## Initializing home before its first write
##


def test_initializeHome_before_first_write(roots, calibrationRoot):
    # e.g. initialize home from the failover before its first write
    home, _ = roots
    store = DefaultIgnoringStore(IndexerType.DEFAULT, calibrationRoot, STATE)

    def initialize():
        writeVersions(home / STATE, [VERSION_START()])

    with mock.patch.object(store, "initializeHome", side_effect=initialize) as initializeHome:
        indexer = Indexer(store)
        initializeHome.assert_not_called()
        # reads don't initialize anything
        assert indexer.currentVersion() == 2
        initializeHome.assert_not_called()

        # numbered after whatever was initialized
        assert indexer.nextVersion() == VERSION_START() + 1
        initializeHome.assert_called_once()
        rec = record(VERSION_START() + 1)
        indexer.writeRecord(rec)
        indexer.writeParameters(rec.calculationParameters)
        # home has an index now: never initialized again
        indexer.nextVersion()
        initializeHome.assert_called_once()
    assert indexer.rootDirectory == home / STATE


def test_initializeHome_nothing_initialized(roots, calibrationRoot):
    # e.g. a new state: nothing to initialize, so the first write is numbered as usual
    home, _ = roots
    store = FailoverIndexStore(IndexerType.DEFAULT, calibrationRoot, "Powder/new")
    with mock.patch.object(store, "initializeHome") as initializeHome:
        indexer = Indexer(store)
        assert indexer.nextVersion() == VERSION_START()
        initializeHome.assert_called_once()
    assert indexer.rootDirectory == home / "Powder/new"


def test_initializeHome_not_while_pinned(calibrationRoot):
    # nested writes are initialized only once
    store = FailoverIndexStore(IndexerType.DEFAULT, calibrationRoot, STATE)
    with mock.patch.object(store, "initializeHome") as initializeHome:
        indexer = Indexer(store)
        rec = record(VERSION_START())
        indexer.writeRecord(rec)
        # `writeRecord` -> `writeIndexedObject`
        initializeHome.assert_called_once()


def test_first_write_warns_once_about_shadowing(roots, calibrationRoot):
    home, prod = roots
    store = FailoverIndexStore(IndexerType.DEFAULT, calibrationRoot, STATE)
    indexer = Indexer(store)
    with mock.patch.object(IndexStoreModule, "logger") as logger:
        indexer.nextVersion()
        indexer.nextVersion()
    logger.warning.assert_called_once()
    message = logger.warning.call_args.args[0]
    assert str(home / STATE) in message
    assert str(prod / STATE) in message


def test_no_shadowing_warning_without_failover(calibrationRoot):
    indexer = Indexer(FailoverIndexStore(IndexerType.DEFAULT, calibrationRoot, "Powder/new"))
    with mock.patch.object(IndexStoreModule, "logger") as logger:
        indexer.nextVersion()
    logger.warning.assert_not_called()


##
## Recovery mode
##


def test_recoveryMode_tolerates_corrupt_home_index(roots, calibrationRoot):
    home, prod = roots
    prodBefore = snapshot(prod)
    writeVersions(home / STATE, [VERSION_START(), 1])
    (home / STATE / INDEX).write_text("{ corrupt")
    store = FailoverIndexStore(IndexerType.DEFAULT, calibrationRoot, STATE, recoveryMode=True)
    indexer = Indexer(store)
    assert indexer.readIndex() == {}
    # recovered from, and written to, home alone
    recovered = indexer.recoverIndex(dryrun=False)
    assert sorted(entry.version for entry in recovered) == [VERSION_START(), 1]
    assert sorted(indexer.readIndex().keys()) == [VERSION_START(), 1]
    assert snapshot(prod) == prodBefore


def test_corrupt_index_raises_without_recoveryMode(roots, calibrationRoot):
    home, _ = roots
    writeVersions(home / STATE, [VERSION_START(), 1])
    indexer = Indexer(FailoverIndexStore(IndexerType.DEFAULT, calibrationRoot, STATE))
    (home / STATE / INDEX).write_text("{ corrupt")
    with pytest.raises(RuntimeError, match="is corrupted, invalid, or missing"):
        indexer.readIndex()
