"""
`LocalDataService` with a read-failover root: reads may come from the failover, writes only ever go to home.

These tests use the real filesystem: a "qa" calibration home, and a "prod" read-failover root.
"""

import importlib
import json
from contextlib import ExitStack
from pathlib import Path
from unittest import mock

import pytest
from util.Config_helpers import Config_override
from util.dao import DAOFactory

from snapred.backend.dao.calibration import CalibrationDefaultRecord
from snapred.backend.dao.calibration.CalibrationHomeMarker import CalibrationHomeMarker
from snapred.backend.dao.indexing.IndexEntry import IndexEntry
from snapred.backend.dao.indexing.Versioning import VERSION_START, VersionState
from snapred.backend.dao.request import CreateIndexEntryRequest
from snapred.backend.dao.state import GroupingMap
from snapred.backend.dao.state.CalibrantSample.Crystallography import Crystallography
from snapred.backend.data import CalibrationRoot as CalibrationRootModule
from snapred.backend.data.CalibrationRoot import (
    ConfiguredCalibrationRoot,
    calibrationRelative,
    calibrationRoot,
    resolveCalibrationPath,
)
from snapred.backend.data.Indexer import Indexer, IndexerType
from snapred.backend.data.IndexStore import IndexStore, InstrumentParameterStore, StateIndexStore
from snapred.backend.data.LocalDataService import LocalDataService
from snapred.backend.error.CalibrationHomeError import CalibrationHomeError
from snapred.meta.mantid.WorkspaceNameGenerator import ValueFormatter as wnvf
from snapred.meta.redantic import parse_file_as, write_model_pretty

LocalDataServiceModule = importlib.import_module(LocalDataService.__module__)

STATE = "0123456789abcdef"
OTHER_STATE = "fedcba9876543210"
RUN = "57514"


def stamp(root: Path, environment: str) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    marker = CalibrationHomeMarker(schemaVersion=1, environment=environment, createdBy="test")
    write_model_pretty(marker, root / CalibrationHomeMarker.FILENAME)
    return root


def snapshot(root: Path):
    return {str(p.relative_to(root)): p.read_bytes() for p in sorted(root.rglob("*")) if p.is_file()}


def statePath(root: Path, state: str = STATE, mode: str = "lite", workflow: str = "diffraction") -> Path:
    return root / "Powder" / state / mode / workflow


def indexEntry(version: int, useLiteMode: bool = True, runNumber: str = RUN) -> IndexEntry:
    return IndexEntry(
        runNumber=runNumber, useLiteMode=useLiteMode, version=version, appliesTo=f">={runNumber}", author="test"
    )


def writeCalibrations(root: Path, versions, state: str = STATE):
    # populate a root independently of any failover: as `initializeState`, then any further calibrations
    for useLiteMode, mode in ((True, "lite"), (False, "native")):
        indexer = Indexer(IndexStore(IndexerType.CALIBRATION, statePath(root, state, mode)))
        for version in versions:
            entry = indexEntry(version, useLiteMode)
            parameters = DAOFactory.calibrationParameters(RUN, useLiteMode, version)
            parameters.indexEntry = entry
            if version == VERSION_START():
                record = CalibrationDefaultRecord(
                    runNumber=RUN,
                    useLiteMode=useLiteMode,
                    version=version,
                    calculationParameters=parameters,
                    workspaces={},
                    indexEntry=entry,
                )
                # stands in for the default diffcal table
                (indexer.versionPath(version)).mkdir(parents=True, exist_ok=True)
                (indexer.versionPath(version) / "default_diffract_consts.h5").write_text("default table")
            else:
                record = DAOFactory.calibrationRecord(RUN, useLiteMode, version)
                record.indexEntry = entry
                record.calculationParameters = parameters
            indexer.addIndexEntry(entry)
            indexer.writeRecord(record)
            indexer.writeParameters(parameters)
    groupingMap = DAOFactory.groupingMap_SNAP(DAOFactory.magical_state_id.model_copy())
    write_model_pretty(groupingMap, root / "Powder" / state / "groupingMap.json")


def writeNormalizations(root: Path, versions, state: str = STATE):
    indexer = Indexer(IndexStore(IndexerType.NORMALIZATION, statePath(root, state, "lite", "normalization")))
    for version in versions:
        entry = indexEntry(version)
        record = DAOFactory.normalizationRecord(RUN, True, version)
        record.indexEntry = entry
        record.calculationParameters.indexEntry = entry
        indexer.addIndexEntry(entry)
        indexer.writeRecord(record)
        indexer.writeParameters(record.calculationParameters)


def writeInstrumentParameters(root: Path, versions):
    indexer = Indexer(IndexStore(IndexerType.INSTRUMENT_PARAMETER, root / "SNAPInstPrm"))
    for version in versions:
        config = DAOFactory.default_instrument_config.model_copy()
        entry = indexEntry(version, runNumber=str(10000 * (version + 1)))
        config.version = version
        config.indexEntry = entry
        indexer.addIndexEntry(entry)
        indexer.writeRecord(config)


@pytest.fixture
def roots(tmp_path):
    home = stamp(tmp_path / "CalibrationQA", "qa")
    prod = stamp(tmp_path / "Calibration", "prod")
    with ExitStack() as stack:
        for key, value in (
            ("instrument.calibration.home", str(home)),
            ("instrument.calibration.environment", "qa"),
            ("instrument.calibration.requireMarker", True),
            ("instrument.calibration.readFailover", {"enabled": True, "paths": {"prod": str(prod)}}),
            ("instrument.calibration.sample.home", str(home / "CalibrantSamples")),
            ("instrument.calibration.powder.grouping.home", str(home / "Powder" / "PixelGroupingDefinitions")),
        ):
            stack.enter_context(Config_override(key, value))
        yield home, prod


@pytest.fixture
def service(roots):  # noqa: ARG001
    instance = LocalDataService()
    states = {RUN: STATE, "12345": OTHER_STATE}
    with mock.patch.object(instance, "generateStateId", side_effect=lambda runId: (states[runId], None)):
        yield instance


@pytest.fixture
def prodState(roots):
    _, prod = roots
    writeCalibrations(prod, [VERSION_START(), 1, 2])
    writeNormalizations(prod, [0, 1])
    return prod


##
## The configured calibration root
##


def test_calibrationRoot(roots):
    home, prod = roots
    root = calibrationRoot()
    assert root.home == home
    assert root.failovers == [prod]
    # cached while the configuration is unchanged
    assert calibrationRoot() is root


def test_calibrationRoot_is_held_by_the_singleton(roots):  # noqa: ARG001
    assert ConfiguredCalibrationRoot() is ConfiguredCalibrationRoot()
    root = calibrationRoot()
    assert ConfiguredCalibrationRoot().root() is root
    ConfiguredCalibrationRoot._reset_Singleton()
    assert calibrationRoot() is not root


def test_calibrationRoot_rebuilt_on_configuration_change(roots, tmp_path):
    root = calibrationRoot()
    other = stamp(tmp_path / "Other", "qa")
    with Config_override("instrument.calibration.home", str(other)):
        assert calibrationRoot().home == other
    assert calibrationRoot() is not root
    assert calibrationRoot().home == roots[0]


def test_calibrationRoot_rebuilt_when_failover_disabled(roots):
    assert calibrationRoot().failovers == [roots[1]]
    with Config_override("instrument.calibration.readFailover", {"enabled": False}):
        assert calibrationRoot().failovers == []


def test_calibrationRoot_rebuilt_when_marker_changes(roots):
    home, _ = roots
    root = calibrationRoot()
    marker = CalibrationHomeMarker(schemaVersion=2, environment="qa", createdBy="test")
    write_model_pretty(marker, home / CalibrationHomeMarker.FILENAME)
    with pytest.raises(CalibrationHomeError, match="schema version 2"):
        calibrationRoot()
    stamp(home, "qa")
    assert calibrationRoot() is not root


def test_calibrationRoot_prod_guard_applies_at_every_call(roots):  # noqa: ARG001
    calibrationRoot()
    with Config_override("instrument.calibration.environment", "prod"):
        with pytest.raises(CalibrationHomeError, match="cannot be enabled for the 'prod' calibration environment"):
            calibrationRoot()


def test_calibrationRelative(roots, tmp_path):
    home, prod = roots
    assert calibrationRelative(home / "Powder" / STATE) == Path("Powder") / STATE
    assert calibrationRelative(home) == Path(".")
    # not under home: used as-is
    assert calibrationRelative(prod / "Powder") is None
    assert calibrationRelative(tmp_path / "elsewhere") is None
    assert calibrationRelative("Powder") is None


def test_resolveCalibrationPath(roots, tmp_path):
    home, prod = roots
    (prod / "Powder").mkdir()
    (prod / "Powder" / "LiteGroupMap.hdf").write_text("prod")
    assert resolveCalibrationPath(home / "Powder" / "LiteGroupMap.hdf") == prod / "Powder" / "LiteGroupMap.hdf"

    (home / "Powder").mkdir()
    (home / "Powder" / "LiteGroupMap.hdf").write_text("home")
    assert resolveCalibrationPath(home / "Powder" / "LiteGroupMap.hdf") == home / "Powder" / "LiteGroupMap.hdf"

    # missing everywhere: the home path, so that callers report it there
    assert resolveCalibrationPath(home / "missing.json") == home / "missing.json"
    # not under home: unchanged
    assert resolveCalibrationPath(tmp_path / "elsewhere") == tmp_path / "elsewhere"


##
## State lookup
##


@pytest.mark.usefixtures("prodState")
def test_stateExists_in_failover(service, roots):
    home, _ = roots
    assert service.stateExists(RUN)
    assert not service.stateExists("12345")
    # nothing is created in home by the lookup
    assert not (home / "Powder").exists()


@pytest.mark.usefixtures("prodState")
def test_checkCalibrationFileExists_in_failover(service, roots):
    home, _ = roots
    assert service.checkCalibrationFileExists(RUN)
    assert not service.checkCalibrationFileExists("12345")
    assert not (home / "Powder").exists()


@pytest.mark.usefixtures("prodState")
def test_calibrationStateFolders(service, roots):
    home, _ = roots
    writeCalibrations(home, [VERSION_START()], state=OTHER_STATE)
    # not a state: no mode folder
    (home / "Powder" / "PixelGroupingDefinitions").mkdir()
    assert service._calibrationStateFolders(True) == sorted([STATE, OTHER_STATE])
    assert service._calibrationStateFolders(False) == sorted([STATE, OTHER_STATE])


def test_calibrationStateFolders_mode(service, roots):
    _, prod = roots
    statePath(prod, STATE, "lite").mkdir(parents=True)
    assert service._calibrationStateFolders(True) == [STATE]
    assert service._calibrationStateFolders(False) == []


def test_calibrationStateFolders_not_under_home(service, tmp_path):
    states = tmp_path / "elsewhere"
    (states / STATE / "native").mkdir(parents=True)
    (states / OTHER_STATE / "lite").mkdir(parents=True)
    (states / "file").write_text("")
    with Config_override("instrument.calibration.powder.home", str(states)):
        assert service._calibrationStateFolders(False) == [STATE]


##
## Indexed reads
##


@pytest.mark.usefixtures("prodState")
def test_calibrationIndexer_reads_failover(service, roots):
    _, prod = roots
    indexer = service.calibrationIndexer(True, STATE)
    assert indexer.store.isFailover
    assert indexer.rootDirectory == statePath(prod)
    assert indexer.currentVersion() == 2
    assert service.readCalibrationRecord(RUN, True, STATE).version == 2
    # the default version is not listed
    assert [entry.version for entry in service.readCalibrationIndex(True, STATE)] == [1, 2]


@pytest.mark.usefixtures("prodState")
def test_calibrationIndexer_home_default_does_not_shadow(service, roots):
    home, _ = roots
    writeCalibrations(home, [VERSION_START()])
    assert service.calibrationIndexer(True, STATE).currentVersion() == 2


@pytest.mark.usefixtures("prodState")
def test_calibrationIndexer_home_version_shadows(service, roots):
    home, _ = roots
    writeCalibrations(home, [VERSION_START(), 1])
    indexer = service.calibrationIndexer(True, STATE)
    assert not indexer.store.isFailover
    assert indexer.currentVersion() == 1


@pytest.mark.usefixtures("prodState")
def test_calibrationIndexer_cached_per_root(service, roots, tmp_path):  # noqa: ARG001
    indexer = service.calibrationIndexer(True, STATE)
    assert service.calibrationIndexer(True, STATE) is indexer
    with Config_override("instrument.calibration.home", str(stamp(tmp_path / "Other", "qa"))):
        other = service.calibrationIndexer(True, STATE)
        assert other is not indexer
        assert other.store.writeDirectory == statePath(tmp_path / "Other")


def test_calibrationIndexer_not_under_home(service, tmp_path):
    with mock.patch.object(service, "_constructCalibrationStatePath", return_value=tmp_path / "elsewhere"):
        indexer = service.calibrationIndexer(True, STATE)
    assert type(indexer.store) is IndexStore
    assert indexer.store.readDirectory == indexer.store.writeDirectory == tmp_path / "elsewhere"


@pytest.mark.usefixtures("prodState")
def test_indexer_stores(service, roots):
    home, _ = roots
    calibrationStore = service.calibrationIndexer(True, STATE).store
    assert isinstance(calibrationStore, StateIndexStore)
    assert calibrationStore.indexerType == IndexerType.CALIBRATION
    assert calibrationStore.homeDirectory == statePath(home)
    normalizationStore = service.normalizationIndexer(False, STATE).store
    assert isinstance(normalizationStore, StateIndexStore)
    assert normalizationStore.indexerType == IndexerType.NORMALIZATION
    assert normalizationStore.homeDirectory == statePath(home, mode="native", workflow="normalization")
    parameterStore = service.instrumentParameterIndexer().store
    assert isinstance(parameterStore, InstrumentParameterStore)
    assert parameterStore.homeDirectory == home / "SNAPInstPrm"


def test_instrumentParameterIndexer_not_under_home(service, tmp_path):
    with Config_override("instrument.parameters.home", str(tmp_path / "elsewhere")):
        indexer = service.instrumentParameterIndexer()
    assert type(indexer.store) is IndexStore
    assert indexer.store.writeDirectory == tmp_path / "elsewhere"


@pytest.mark.usefixtures("prodState")
def test_normalizationIndexer_reads_failover(service, roots):
    _, prod = roots
    indexer = service.normalizationIndexer(True, STATE)
    assert indexer.rootDirectory == statePath(prod, workflow="normalization")
    assert indexer.currentVersion() == 1


@pytest.mark.usefixtures("prodState")
def test_normalizationIndexer_home_version_0_shadows(service, roots):
    # a normalization has no default version: its version 0 is a real normalization
    home, _ = roots
    writeNormalizations(home, [0])
    indexer = service.normalizationIndexer(True, STATE)
    assert not indexer.store.isFailover
    assert indexer.currentVersion() == 0


def test_instrumentParameterIndexer_reads_failover(service, roots):
    home, prod = roots
    writeInstrumentParameters(prod, [0, 1])
    indexer = service.instrumentParameterIndexer()
    assert indexer.store.isFailover
    assert indexer.store.writeDirectory == home / "SNAPInstPrm"
    assert service.readInstrumentParametersVersion(1).version == 1


##
## Single-file and listing reads
##


@pytest.mark.usefixtures("prodState")
def test_readStateConfig_grouping_map_from_failover(service, roots):
    home, prod = roots
    before = snapshot(prod)
    with mock.patch.object(service, "_prepareStateRoot") as mockPrepareStateRoot:
        with mock.patch.object(GroupingMap, "_asAbsolutePath", side_effect=lambda path: path):
            stateConfig = service.readStateConfig(RUN, True)
    # a read never creates anything in home
    mockPrepareStateRoot.assert_not_called()
    assert not (home / "Powder").exists()
    assert stateConfig.calibration.version == 2
    assert snapshot(prod) == before


def test_readStateConfig_prepares_state_missing_everywhere(service, roots):  # noqa: ARG001
    indexer = mock.Mock()
    with (
        mock.patch.object(service, "calibrationIndexer", return_value=indexer),
        mock.patch.object(service, "_prepareStateRoot") as mockPrepareStateRoot,
        mock.patch.object(service, "_readGroupingMap") as mockReadGroupingMap,
        mock.patch.object(LocalDataServiceModule, "StateConfig"),
    ):
        service.readStateConfig(RUN, True)
    mockPrepareStateRoot.assert_called_once_with(STATE)
    mockReadGroupingMap.assert_called_once_with(STATE)


@pytest.mark.usefixtures("prodState")
def test_readGroupingMap_from_failover(service, roots):
    _, prod = roots
    with mock.patch.object(LocalDataServiceModule, "parse_file_as") as mockParse:
        service._readGroupingMap(STATE)
    mockParse.assert_called_once_with(GroupingMap, prod / "Powder" / STATE / "groupingMap.json")


def test_readDefaultGroupingMap_from_failover(service, roots):
    _, prod = roots
    default = prod / "Powder" / "PixelGroupingDefinitions" / "defaultGroupingMap.json"
    default.parent.mkdir(parents=True)
    default.write_text("{}")
    with mock.patch.object(LocalDataServiceModule, "parse_file_as") as mockParse:
        service._readDefaultGroupingMap()
    mockParse.assert_called_once_with(GroupingMap, default)


def test_readDefaultGroupingMap_missing_everywhere(service, roots):
    home, _ = roots
    with pytest.raises(FileNotFoundError, match=str(home)):
        service._readDefaultGroupingMap()


def test_GroupingMap_definition_from_failover(roots):
    home, prod = roots
    definitions = prod / "Powder" / "PixelGroupingDefinitions"
    definitions.mkdir(parents=True)
    (definitions / "Column.lite.xml").write_text("")
    assert GroupingMap._asAbsolutePath(Path("Column.lite.xml")) == definitions / "Column.lite.xml"
    # missing everywhere: the home path
    assert GroupingMap._asAbsolutePath(Path("Bank.lite.xml")) == (
        home / "Powder" / "PixelGroupingDefinitions" / "Bank.lite.xml"
    )
    assert GroupingMap._asAbsolutePath(Path("/absolute.xml")) == Path("/absolute.xml")


def test_readSampleFilePaths_union(service, roots):
    home, prod = roots
    for root, names in ((home, ["a.json", "b.json"]), (prod, ["b.json", "c.json", "ignored.txt"])):
        samples = root / "CalibrantSamples"
        samples.mkdir()
        for name in names:
            (samples / name).write_text(root.name)
    assert service.readSampleFilePaths() == sorted(
        [
            str(home / "CalibrantSamples" / "a.json"),
            # home wins
            str(home / "CalibrantSamples" / "b.json"),
            str(prod / "CalibrantSamples" / "c.json"),
        ]
    )


def test_readSampleFilePaths_none(service, roots):
    _, prod = roots
    (prod / "CalibrantSamples").mkdir()
    with pytest.raises(RuntimeError, match="No samples found"):
        service.readSampleFilePaths()


def test_readCifFilePath_from_failover(service, roots):
    _, prod = roots
    samples = prod / "CalibrantSamples"
    samples.mkdir()
    (samples / "Silicon.json").write_text(json.dumps({"crystallography": {"cifFile": "Silicon.cif"}}))
    (samples / "Silicon.cif").write_text("")
    assert service.readCifFilePath("Silicon") == str(samples / "Silicon.cif")


def test_readCifFilePath_missing(service, roots):
    home, _ = roots
    with pytest.raises(ValueError, match=str(home / "CalibrantSamples" / "Silicon.json")):
        service.readCifFilePath("Silicon")


def test_Crystallography_cifFile_from_failover(roots):
    _, prod = roots
    samples = prod / "CalibrantSamples"
    samples.mkdir()
    (samples / "Silicon.cif").write_text("")
    assert Crystallography.validate_cifFile("Silicon.cif") == "Silicon.cif"
    with pytest.raises(ValueError, match="must either be a relative path"):
        Crystallography.validate_cifFile("Missing.cif")


def test_lite_map_from_failover(roots):
    _, prod = roots
    (prod / "Powder").mkdir()
    (prod / "Powder" / "LiteGroupMap.hdf").write_text("")
    from snapred.backend.data.GroceryService import GroceryService

    assert GroceryService()._createGroupingFilename(RUN, "Lite", True) == str(prod / "Powder" / "LiteGroupMap.hdf")


def test_calibrant_sample_threshold_from_failover(roots):
    _, prod = roots
    (prod / "CalibrantSamples").mkdir()
    (prod / "CalibrantSamples" / "Silicon.json").write_text("{}")
    from snapred.backend.service.SousChef import SousChef

    sousChef = SousChef()
    with mock.patch.object(sousChef, "prepCalibrantSample") as mockPrepCalibrantSample:
        threshold = sousChef._getThresholdFromCalibrantSample("Silicon.json")
    mockPrepCalibrantSample.assert_called_once_with(str(prod / "CalibrantSamples" / "Silicon.json"))
    assert threshold == mockPrepCalibrantSample.return_value.peakIntensityFractionThreshold


##
## Writes
##


@pytest.mark.usefixtures("prodState")
def test_write_to_failover_state_initializes_home(service, roots):
    home, prod = roots
    before = snapshot(prod)

    entry = service.createCalibrationIndexEntry(
        CreateIndexEntryRequest(
            runNumber=RUN, useLiteMode=True, version=VersionState.NEXT, appliesTo=f">={RUN}", comments="", author="test"
        )
    )
    # numbered in home, after home's copy of the default version
    assert entry.version == VERSION_START() + 1

    # the state is initialized in home from the failover: grouping-schema map and default version, in both modes
    assert (home / "Powder" / STATE / "groupingMap.json").read_bytes() == (
        prod / "Powder" / STATE / "groupingMap.json"
    ).read_bytes()
    for mode in ("lite", "native"):
        index = parse_file_as(list[IndexEntry], statePath(home, mode=mode) / "CalibrationIndex.json")
        assert [e.version for e in index] == [VERSION_START()]
        versionPath = statePath(home, mode=mode) / wnvf.pathVersion(VERSION_START())
        assert (versionPath / "default_diffract_consts.h5").read_text() == "default table"
        assert not (statePath(home, mode=mode) / wnvf.pathVersion(1)).exists()
    # normalization is not copied
    assert not statePath(home, workflow="normalization").exists()
    # nothing is written to the failover
    assert snapshot(prod) == before


@pytest.mark.usefixtures("prodState")
def test_write_to_failover_state_then_shadows(service, roots):
    home, prod = roots
    before = snapshot(prod)
    record = DAOFactory.calibrationRecord(RUN, True, 1)
    entry = indexEntry(1)
    record.indexEntry = entry
    record.calculationParameters.indexEntry = entry

    indexer = service.calibrationIndexer(True, STATE)
    assert indexer.store.isFailover
    version = indexer.nextVersion()
    assert version == 1
    service.writeCalibrationIndexEntry(entry)
    service.writeCalibrationRecord(record)

    assert not indexer.store.isFailover
    assert indexer.currentVersion() == 1
    assert service.readCalibrationRecord(RUN, True, STATE).version == 1
    assert (statePath(home) / wnvf.pathVersion(1)).is_dir()
    # native still reads prod: it has only its default version in home
    assert service.calibrationIndexer(False, STATE).currentVersion() == 2
    assert snapshot(prod) == before


@pytest.mark.usefixtures("prodState")
def test_normalization_write_to_failover_state_initializes_home(service, roots):
    home, prod = roots
    before = snapshot(prod)
    indexer = service.normalizationIndexer(True, STATE)
    # a normalization is numbered from home's normalizations alone
    assert indexer.nextVersion() == 0
    assert (home / "Powder" / STATE / "groupingMap.json").exists()
    assert (statePath(home) / "CalibrationIndex.json").exists()
    assert snapshot(prod) == before


@pytest.mark.usefixtures("prodState")
def test_write_initializes_home_only_once(service, roots):
    home, _ = roots
    service.calibrationIndexer(True, STATE).nextVersion()
    marker = statePath(home) / wnvf.pathVersion(VERSION_START()) / "default_diffract_consts.h5"
    marker.write_text("modified in home")
    (home / "Powder" / STATE / "groupingMap.json").write_text("{}")
    service.calibrationIndexer(True, STATE).nextVersion()
    service.normalizationIndexer(True, STATE).nextVersion()
    assert marker.read_text() == "modified in home"
    assert (home / "Powder" / STATE / "groupingMap.json").read_text() == "{}"


@pytest.mark.usefixtures("prodState")
def test_write_to_new_state_does_not_initialize(service, roots):
    home, _ = roots
    # a state in no failover: initialized as usual, by `initializeState`
    assert service.calibrationIndexer(True, OTHER_STATE).nextVersion() == VERSION_START()
    assert not (home / "Powder" / OTHER_STATE).exists()


@pytest.mark.usefixtures("prodState")
def test_state_initialization_no_failover(service, roots):
    home, _ = roots
    with Config_override("instrument.calibration.readFailover", {"enabled": False}):
        service.calibrationIndexer(True, STATE).store.prepareWrite()
    assert not (home / "Powder" / STATE / "groupingMap.json").exists()
    assert not (statePath(home) / "CalibrationIndex.json").exists()


@pytest.mark.usefixtures("prodState")
def test_state_initialization_not_under_home(service, roots, tmp_path):
    home, _ = roots
    with mock.patch.object(service, "constructCalibrationStateRoot", return_value=tmp_path / "elsewhere"):
        store = service.calibrationIndexer(True, STATE).store
    assert type(store) is IndexStore
    store.prepareWrite()
    assert not (home / "Powder").exists()


def test_state_initialization_partial_failover_state(service, roots):
    # e.g. a failover state with only one mode, or without a default version
    home, prod = roots
    writeCalibrations(prod, [1])
    (statePath(prod, mode="native") / "CalibrationIndex.json").unlink()
    service.calibrationIndexer(True, STATE).store.prepareWrite()
    assert (home / "Powder" / STATE / "groupingMap.json").exists()
    assert not (statePath(home) / "CalibrationIndex.json").exists()
    assert not (statePath(home, mode="native") / "CalibrationIndex.json").exists()


@pytest.mark.usefixtures("prodState")
def test_state_initialization_no_temporary_index(service, roots):
    home, _ = roots
    service.calibrationIndexer(True, STATE).store.prepareWrite()
    assert not list(home.rglob("*.tmp"))


@pytest.mark.usefixtures("prodState")
def test_copyCalibration_from_failover(service, roots):
    home, prod = roots
    before = snapshot(prod)
    target = indexEntry(VERSION_START(), runNumber="12345")
    # the source is read from prod: it has no workspace files, so only the records are copied
    sourceRecord = service.calibrationIndexer(True, STATE).readRecord(2)
    sourceRecord.workspaces = {}
    with mock.patch.object(Indexer, "readRecord", return_value=sourceRecord):
        service.copyCalibration(STATE, OTHER_STATE, target, 2)
    assert target.version == VERSION_START()
    assert (statePath(home, OTHER_STATE) / wnvf.pathVersion(VERSION_START())).is_dir()
    assert not (prod / "Powder" / OTHER_STATE).exists()
    assert snapshot(prod) == before


def test_instrument_parameters_write_copies_failover(service, roots):
    home, prod = roots
    writeInstrumentParameters(prod, [0, 1])
    before = snapshot(prod)
    config = DAOFactory.default_instrument_config.model_copy()
    service.writeInstrumentParameters(config, ">=30000", "test")

    # home has all of prod's epochs, followed by the new one
    indexer = service.instrumentParameterIndexer()
    assert not indexer.store.isFailover
    assert sorted(indexer.index) == [0, 1, 2]
    for version in (0, 1):
        assert (home / "SNAPInstPrm" / wnvf.pathVersion(version)).is_dir()
    assert snapshot(prod) == before


def test_instrument_parameters_write_without_failover(service, roots):
    home, prod = roots
    service.writeInstrumentParameters(DAOFactory.default_instrument_config.model_copy(), ">=30000", "test")
    assert sorted(service.instrumentParameterIndexer().index) == [0]
    assert (home / "SNAPInstPrm" / "InstrumentParameterIndex.json").exists()
    assert not (prod / "SNAPInstPrm").exists()


def test_instrument_parameters_initialization_not_under_home(service, roots, tmp_path):
    home, prod = roots
    writeInstrumentParameters(prod, [0])
    with Config_override("instrument.parameters.home", str(tmp_path / "elsewhere")):
        service.instrumentParameterIndexer().store.prepareWrite()
    assert not (home / "SNAPInstPrm").exists()


@pytest.mark.usefixtures("prodState")
def test_writes_never_land_in_failover(service, roots):
    # the write guard: a symlink from home into the failover is refused
    home, prod = roots
    (home / "Powder").mkdir()
    (home / "Powder" / STATE).symlink_to(prod / "Powder" / STATE)
    before = snapshot(prod)
    with pytest.raises(CalibrationRootModule.CalibrationWriteGuardError):
        service.calibrationIndexer(True, STATE).nextVersion()
    assert snapshot(prod) == before
