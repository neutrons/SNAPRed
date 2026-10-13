from contextlib import ExitStack
from pathlib import Path
from unittest import mock

import pytest
from util.Config_helpers import Config_override

from snapred.backend.dao.calibration.CalibrationHomeMarker import CalibrationEnvironment, CalibrationHomeMarker
from snapred.backend.data import CalibrationRoot as CalibrationRootModule
from snapred.backend.data.CalibrationRoot import (
    CalibrationRoot,
    deployEnvironment,
    isUserMode,
    readFailoverPaths,
)
from snapred.backend.error.CalibrationHomeError import CalibrationHomeError
from snapred.backend.error.CalibrationWriteGuardError import CalibrationWriteGuardError
from snapred.meta.Config import Config
from snapred.meta.redantic import write_model_pretty

SCHEMA = 1


def stamp(root: Path, environment: str, schemaVersion: int = SCHEMA) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    marker = CalibrationHomeMarker(schemaVersion=schemaVersion, environment=environment, createdBy="test")
    write_model_pretty(marker, root / CalibrationHomeMarker.FILENAME)
    return root


def touch(path: Path, text: str = "") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
    return path


@pytest.fixture
def mockLogger():
    with mock.patch.object(CalibrationRootModule, "logger") as logger:
        yield logger


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


class FakeConfig:
    """Stand-in for `Config` that records which keys are read, and has no defaults for missing keys."""

    def __init__(self, values: dict, version: str = "2.4.0rc1"):
        self.values = values
        self.version = version
        self.reads = []

    def __getitem__(self, key):
        self.reads.append(key)
        return self.values[key]

    def snapredVersion(self):
        return self.version


@pytest.fixture
def fakeConfig():
    def _fakeConfig(values: dict, version: str = "2.4.0rc1", testEnv: bool = False):
        config = FakeConfig({"environment": "snapred_qa", **values}, version)
        stack.enter_context(mock.patch.object(CalibrationRootModule, "Config", config))
        stack.enter_context(mock.patch.object(CalibrationRootModule, "isTestEnv", return_value=testEnv))
        return config

    with ExitStack() as stack:
        yield _fakeConfig


ENABLED = "instrument.calibration.readFailover.enabled"
PATHS = "instrument.calibration.readFailover.paths"
CAL_ENV = "instrument.calibration.environment"

##
## Environment detection
##


@pytest.mark.parametrize(
    ("version", "expected"),
    [
        ("2.4.0+exp.1", CalibrationEnvironment.EXP),
        ("2.4.0+exp.1.cycle.aware.calib", CalibrationEnvironment.EXP),
        # exp is checked before dev: an exp build must never be classified as next
        ("2.4.1.dev3+exp.1", CalibrationEnvironment.EXP),
        ("2.4.1.dev3", CalibrationEnvironment.NEXT),
        ("2.4.0rc1", CalibrationEnvironment.QA),
        ("2.4.0", CalibrationEnvironment.PROD),
    ],
)
def test_deployEnvironment(version, expected):
    with mock.patch.object(Config, "snapredVersion", return_value=version):
        assert deployEnvironment() == expected


@pytest.mark.parametrize(("environment", "expected"), [("snapred-user", True), ("test", False), ("snapred_qa", False)])
def test_isUserMode(environment, expected):
    with Config_override("environment", environment):
        assert isUserMode() is expected


##
## Read order and prod guard
##


def test_readFailoverPaths_userMode_reads_no_failover_keys(fakeConfig):
    # the deployed config is copied into "snapred-user.yml": user mode must ignore it
    config = fakeConfig({"environment": "snapred-user", ENABLED: True, PATHS: {"prod": "/prod"}})
    assert readFailoverPaths() == []
    assert not any(key.startswith("instrument.calibration") for key in config.reads)


def test_readFailoverPaths_userMode_in_prod_deployment(fakeConfig):
    fakeConfig({"environment": "snapred-user", ENABLED: True, CAL_ENV: "prod"}, version="2.4.0")
    assert readFailoverPaths() == []


def test_readFailoverPaths_disabled_does_not_read_paths(fakeConfig):
    # no "paths" key: reading it would raise `KeyError`
    config = fakeConfig({ENABLED: False, CAL_ENV: "qa"})
    assert readFailoverPaths() == []
    assert PATHS not in config.reads


def test_readFailoverPaths_disabled_in_prod_deployment(fakeConfig):
    fakeConfig({ENABLED: False, CAL_ENV: "prod"}, version="2.4.0")
    assert readFailoverPaths() == []


@pytest.mark.parametrize("enabled", ["false", "true", 0, 1, None])
def test_readFailoverPaths_enabled_must_be_bool(fakeConfig, enabled):
    config = fakeConfig({ENABLED: enabled, CAL_ENV: "qa", PATHS: {"prod": "/prod"}})
    with pytest.raises(CalibrationHomeError, match="must be a boolean"):
        readFailoverPaths()
    assert PATHS not in config.reads


def test_readFailoverPaths_enabled(fakeConfig):
    config = fakeConfig({ENABLED: True, CAL_ENV: "qa", PATHS: {"prod": "/a/prod", "next": "/a/next"}})
    assert readFailoverPaths() == [Path("/a/prod"), Path("/a/next")]
    # read order: "enabled" before "paths"
    assert config.reads.index(ENABLED) < config.reads.index(PATHS)


def test_readFailoverPaths_enabled_without_paths(fakeConfig):
    # no defaults: `Config`'s normal `KeyError`
    fakeConfig({ENABLED: True, CAL_ENV: "qa"})
    with pytest.raises(KeyError):
        readFailoverPaths()


def test_readFailoverPaths_paths_must_be_mapping(fakeConfig):
    fakeConfig({ENABLED: True, CAL_ENV: "qa", PATHS: ["/a/prod"]})
    with pytest.raises(CalibrationHomeError, match="mapping"):
        readFailoverPaths()


def test_readFailoverPaths_refused_for_prod_calibration_environment(fakeConfig):
    # refused even for a non-prod deployment, and in a test environment
    config = fakeConfig({ENABLED: True, CAL_ENV: "prod", PATHS: {"prod": "/prod"}}, version="2.4.0rc1", testEnv=True)
    with pytest.raises(CalibrationHomeError, match="'prod' calibration environment"):
        readFailoverPaths()
    assert PATHS not in config.reads


@pytest.mark.parametrize("calibrationEnvironment", ["qa", "next", "exp"])
def test_readFailoverPaths_refused_in_prod_deployment(fakeConfig, calibrationEnvironment):
    # the deployment is detected from the installed version, regardless of what the yml says
    config = fakeConfig(
        {ENABLED: True, CAL_ENV: calibrationEnvironment, PATHS: {"prod": "/prod"}},
        version="2.4.0",
    )
    with pytest.raises(CalibrationHomeError, match="prod deployment"):
        readFailoverPaths()
    assert PATHS not in config.reads


@pytest.mark.parametrize("version", ["2.4.0+exp.1", "2.4.1.dev3", "2.4.0rc1"])
def test_readFailoverPaths_allowed_in_testing_deployments(fakeConfig, version):
    fakeConfig({ENABLED: True, CAL_ENV: "qa", PATHS: {"prod": "/prod"}}, version=version)
    assert readFailoverPaths() == [Path("/prod")]


def test_readFailoverPaths_allowed_in_test_environment(fakeConfig):
    # tests run with a "prod" version
    fakeConfig({ENABLED: True, CAL_ENV: "qa", PATHS: {"prod": "/prod"}}, version="2.4.0", testEnv=True)
    assert readFailoverPaths() == [Path("/prod")]


def test_readFailoverPaths_Config():
    # with the real `Config`: the mapping's members are interpolated
    instrumentHome = Config["instrument.home"]
    failover = {
        "enabled": True,
        "paths": {
            "prod": "${instrument.home}/shared/Calibration",
            "next": "${instrument.home}/shared/CalibrationNext",
        },
    }
    with (
        Config_override("instrument.calibration.readFailover", failover),
        Config_override(CAL_ENV, "qa"),
    ):
        assert readFailoverPaths() == [
            Path(f"{instrumentHome}/shared/Calibration"),
            Path(f"{instrumentHome}/shared/CalibrationNext"),
        ]


def test_readFailoverPaths_Config_default():
    # the test config has failover disabled
    assert readFailoverPaths() == []


##
## Construction: home
##


def test_home_valid(roots):
    home, _, _ = roots
    root = CalibrationRoot(home, environment="qa", schemaVersion=SCHEMA)
    assert root.home == home
    assert root.roots == [home]
    assert root.failovers == []
    assert root.environment == CalibrationEnvironment.QA


def test_home_missing_marker_required(tmp_path):
    with pytest.raises(CalibrationHomeError, match="no marker file"):
        CalibrationRoot(tmp_path, environment="qa", schemaVersion=SCHEMA, requireMarker=True)


def test_home_missing_marker_grace(tmp_path, mockLogger):
    root = CalibrationRoot(tmp_path, environment="qa", schemaVersion=SCHEMA, requireMarker=False)
    assert root.roots == [tmp_path]
    mockLogger.warning.assert_called_once()
    assert "no marker file" in mockLogger.warning.call_args[0][0]


def test_home_does_not_exist_grace(tmp_path, mockLogger):
    home = tmp_path / "doesNotExist"
    CalibrationRoot(home, environment="qa", schemaVersion=SCHEMA, requireMarker=False)
    mockLogger.warning.assert_called_once()
    assert not home.exists()


def test_home_schema_mismatch(tmp_path):
    stamp(tmp_path, "qa", schemaVersion=SCHEMA + 1)
    with pytest.raises(CalibrationHomeError, match="schema version"):
        CalibrationRoot(tmp_path, environment="qa", schemaVersion=SCHEMA, requireMarker=False)


def test_home_environment_mismatch(tmp_path):
    # e.g. a QA deployment whose home was misconfigured to point at prod
    stamp(tmp_path, "prod")
    with pytest.raises(CalibrationHomeError, match="stamped for the 'prod' environment"):
        CalibrationRoot(tmp_path, environment="qa", schemaVersion=SCHEMA, requireMarker=False)


def test_home_corrupt_marker(tmp_path):
    touch(tmp_path / CalibrationHomeMarker.FILENAME, "{ not json")
    with pytest.raises(CalibrationHomeError, match="Unable to read"):
        CalibrationRoot(tmp_path, environment="qa", schemaVersion=SCHEMA, requireMarker=False)


def test_invalid_environment(tmp_path):
    with pytest.raises(ValueError, match="'user' is not a valid CalibrationEnvironment"):
        CalibrationRoot(tmp_path, environment="user", schemaVersion=SCHEMA, requireMarker=False)


##
## Construction: user mode
##


def test_userMode_skips_home_marker_check(tmp_path, mockLogger):
    root = CalibrationRoot(tmp_path, environment="qa", schemaVersion=SCHEMA, requireMarker=True, userMode=True)
    assert root.roots == [tmp_path]
    mockLogger.warning.assert_not_called()


def test_userMode_skips_home_schema_and_environment_check(tmp_path):
    stamp(tmp_path, "next", schemaVersion=SCHEMA + 1)
    CalibrationRoot(tmp_path, environment="qa", schemaVersion=SCHEMA, userMode=True)


def test_userMode_corrupt_home_marker(tmp_path):
    touch(tmp_path / CalibrationHomeMarker.FILENAME, "{ not json")
    with pytest.raises(CalibrationHomeError, match="Unable to read"):
        CalibrationRoot(tmp_path, environment="qa", schemaVersion=SCHEMA, userMode=True)


@pytest.mark.usefixtures("mockLogger")
def test_userMode_still_validates_failovers(tmp_path):
    home = tmp_path / "user"
    home.mkdir()
    failover = tmp_path / "unstamped"
    failover.mkdir()
    root = CalibrationRoot(home, [failover], environment="qa", schemaVersion=SCHEMA, userMode=True)
    assert root.failovers == []


##
## Construction: failovers
##


def test_failovers_valid(roots, calibrationRoot):
    home, prod, next_ = roots
    assert calibrationRoot.failovers == [prod, next_]
    assert calibrationRoot.roots == [home, prod, next_]


def test_failovers_priority_order(roots):
    home, prod, next_ = roots
    root = CalibrationRoot(home, [next_, prod], environment="qa", schemaVersion=SCHEMA)
    assert root.roots == [home, next_, prod]


def test_failover_missing_marker_skipped(roots, tmp_path, mockLogger):
    home, prod, _ = roots
    unstamped = tmp_path / "unstamped"
    unstamped.mkdir()
    root = CalibrationRoot(home, [unstamped, prod], environment="qa", schemaVersion=SCHEMA)
    assert root.failovers == [prod]
    assert "no marker file" in mockLogger.warning.call_args[0][0]


def test_failover_does_not_exist_skipped(roots, tmp_path, mockLogger):
    home, prod, _ = roots
    root = CalibrationRoot(home, [tmp_path / "doesNotExist", prod], environment="qa", schemaVersion=SCHEMA)
    assert root.failovers == [prod]
    mockLogger.warning.assert_called_once()


def test_failover_schema_mismatch_skipped(roots, mockLogger):
    home, prod, next_ = roots
    stamp(prod, "prod", schemaVersion=SCHEMA + 1)
    root = CalibrationRoot(home, [prod, next_], environment="qa", schemaVersion=SCHEMA)
    assert root.failovers == [next_]
    assert "schema version" in mockLogger.warning.call_args[0][0]


def test_failover_corrupt_marker_skipped(roots, mockLogger):
    home, prod, next_ = roots
    touch(prod / CalibrationHomeMarker.FILENAME, '{"schemaVersion": 1}')
    root = CalibrationRoot(home, [prod, next_], environment="qa", schemaVersion=SCHEMA)
    assert root.failovers == [next_]
    assert "unable to read its marker" in mockLogger.warning.call_args[0][0]


def test_failover_environment_mismatch_allowed(roots):
    # failovers are expected to belong to another environment
    home, prod, _ = roots
    root = CalibrationRoot(home, [prod], environment="qa", schemaVersion=SCHEMA)
    assert root.failovers == [prod]


def test_failover_duplicate(roots, mockLogger):
    home, prod, _ = roots
    root = CalibrationRoot(home, [prod, Path(str(prod) + "/")], environment="qa", schemaVersion=SCHEMA)
    assert root.failovers == [prod]
    assert "duplicate" in mockLogger.warning.call_args[0][0]


def test_failover_symlinked_duplicate(roots, tmp_path, mockLogger):
    home, prod, _ = roots
    link = tmp_path / "prodLink"
    link.symlink_to(prod)
    root = CalibrationRoot(home, [prod, link], environment="qa", schemaVersion=SCHEMA)
    assert root.failovers == [prod]
    assert "duplicate" in mockLogger.warning.call_args[0][0]


def test_failover_equal_to_home(roots):
    home, _, _ = roots
    with pytest.raises(CalibrationHomeError, match="must not be, contain, or be contained by"):
        CalibrationRoot(home, [home], environment="qa", schemaVersion=SCHEMA)


def test_failover_symlink_to_home(roots, tmp_path):
    home, _, _ = roots
    link = tmp_path / "homeLink"
    link.symlink_to(home)
    with pytest.raises(CalibrationHomeError, match="must not be, contain, or be contained by"):
        CalibrationRoot(home, [link], environment="qa", schemaVersion=SCHEMA)


def test_failover_contains_home(roots, tmp_path):
    home, _, _ = roots
    with pytest.raises(CalibrationHomeError, match="must not be, contain, or be contained by"):
        CalibrationRoot(home, [tmp_path], environment="qa", schemaVersion=SCHEMA)


def test_failover_contained_by_home(roots):
    home, _, _ = roots
    nested = stamp(home / "nested", "prod")
    with pytest.raises(CalibrationHomeError, match="must not be, contain, or be contained by"):
        CalibrationRoot(home, [nested], environment="qa", schemaVersion=SCHEMA)


##
## `fromConfig`
##


def test_fromConfig(roots):
    home, prod, next_ = roots
    failover = {"enabled": True, "paths": {"prod": str(prod), "next": str(next_)}}
    with (
        Config_override("instrument.calibration.home", str(home)),
        Config_override(CAL_ENV, "qa"),
        Config_override("instrument.calibration.requireMarker", True),
        Config_override("instrument.calibration.readFailover", failover),
    ):
        root = CalibrationRoot.fromConfig()
    assert root.roots == [home, prod, next_]
    assert root.environment == CalibrationEnvironment.QA


@pytest.mark.usefixtures("mockLogger")
def test_fromConfig_default(tmp_path):
    # the test config: failover disabled, and a missing marker only warns
    with Config_override("instrument.calibration.home", str(tmp_path)):
        root = CalibrationRoot.fromConfig()
    assert root.roots == [tmp_path]
    assert root.environment == CalibrationEnvironment.PROD


def test_fromConfig_userMode(roots):
    # in user mode, the copied-in failover config is ignored, and the home is not checked
    home, prod, _ = roots
    failover = {"enabled": True, "paths": {"prod": str(prod)}}
    with (
        Config_override("environment", "snapred-user"),
        Config_override("instrument.calibration.home", str(home)),
        Config_override(CAL_ENV, "next"),
        Config_override("instrument.calibration.requireMarker", True),
        Config_override("instrument.calibration.readFailover", failover),
    ):
        root = CalibrationRoot.fromConfig()
    assert root.roots == [home]


##
## Reads
##


def test_resolve_home_first(roots, calibrationRoot):
    home, prod, next_ = roots
    for root in roots:
        touch(root / "Powder" / "LiteGroupMap.hdf")
    assert calibrationRoot.resolve("Powder/LiteGroupMap.hdf") == home / "Powder" / "LiteGroupMap.hdf"


def test_resolve_falls_through_when_missing(roots, calibrationRoot):
    home, prod, next_ = roots
    touch(prod / "a.json")
    touch(next_ / "a.json")
    touch(next_ / "b.json")
    assert calibrationRoot.resolve(Path("a.json")) == prod / "a.json"
    assert calibrationRoot.resolve("b.json") == next_ / "b.json"


def test_resolve_does_not_mask_corrupt_home_file(roots, calibrationRoot):
    # resolve by existence: a corrupt file in home is returned, never silently replaced by a failover
    home, prod, _ = roots
    touch(home / "groupingMap.json", "{ corrupt")
    touch(prod / "groupingMap.json", "{}")
    assert calibrationRoot.resolve("groupingMap.json") == home / "groupingMap.json"


def test_resolve_directory(roots, calibrationRoot):
    _, prod, _ = roots
    (prod / "SNAPInstPrm").mkdir()
    assert calibrationRoot.resolve("SNAPInstPrm") == prod / "SNAPInstPrm"


def test_resolve_not_found(roots, calibrationRoot):
    home, prod, next_ = roots
    with pytest.raises(FileNotFoundError, match="not found in any calibration root") as e:
        calibrationRoot.resolve("missing.json")
    for root in roots:
        assert str(root) in str(e.value)


def test_resolve_skips_invalid_failover(roots, tmp_path):
    home, _, _ = roots
    unstamped = tmp_path / "unstamped"
    touch(unstamped / "a.json")
    root = CalibrationRoot(home, [unstamped], environment="qa", schemaVersion=SCHEMA)
    with pytest.raises(FileNotFoundError):
        root.resolve("a.json")


@pytest.mark.parametrize("rel", ["/abs/a.json", "../a.json", "Powder/../../a.json"])
def test_reads_require_relative_paths(calibrationRoot, rel):
    with pytest.raises(ValueError, match="relative"):
        calibrationRoot.resolve(rel)
    with pytest.raises(ValueError, match="relative"):
        calibrationRoot.exists(rel)
    with pytest.raises(ValueError, match="relative"):
        calibrationRoot.listDirs(rel)
    with pytest.raises(ValueError, match="relative"):
        calibrationRoot.glob(rel)
    with pytest.raises(ValueError, match="relative"):
        calibrationRoot.writePath(rel)


def test_exists(roots, calibrationRoot):
    home, prod, next_ = roots
    touch(home / "a.json")
    touch(next_ / "b.json")
    assert calibrationRoot.exists("a.json")
    assert calibrationRoot.exists("b.json")
    assert not calibrationRoot.exists("c.json")


def test_glob_union_home_wins(roots, calibrationRoot):
    home, prod, next_ = roots
    touch(home / "CalibrantSamples" / "Silicon.json")
    touch(prod / "CalibrantSamples" / "Silicon.json")
    touch(prod / "CalibrantSamples" / "Diamond.json")
    touch(next_ / "CalibrantSamples" / "Diamond.json")
    touch(next_ / "CalibrantSamples" / "CeO2.json")
    touch(next_ / "CalibrantSamples" / "notASample.txt")
    assert calibrationRoot.glob("CalibrantSamples/*.json") == [
        home / "CalibrantSamples" / "Silicon.json",
        prod / "CalibrantSamples" / "Diamond.json",
        next_ / "CalibrantSamples" / "CeO2.json",
    ]


def test_glob_no_matches(calibrationRoot):
    assert calibrationRoot.glob("CalibrantSamples/*.json") == []


def test_listDirs_union(roots, calibrationRoot):
    home, prod, next_ = roots
    (home / "Powder" / "aaa").mkdir(parents=True)
    (prod / "Powder" / "ccc").mkdir(parents=True)
    (prod / "Powder" / "aaa").mkdir(parents=True)
    (next_ / "Powder" / "bbb").mkdir(parents=True)
    touch(next_ / "Powder" / "LiteGroupMap.hdf")
    assert calibrationRoot.listDirs("Powder") == ["aaa", "bbb", "ccc"]


def test_listDirs_missing(calibrationRoot):
    assert calibrationRoot.listDirs("Powder") == []


##
## Writes
##


def test_writePath(roots, calibrationRoot):
    home, prod, next_ = roots
    touch(prod / "CalibrantSamples" / "Silicon.json")
    path = calibrationRoot.writePath("CalibrantSamples/Silicon.json")
    # writes always go to home, even when the file only exists in a failover
    assert path == home / "CalibrantSamples" / "Silicon.json"
    # `writePath` does not create anything
    assert not (home / "CalibrantSamples").exists()


def test_writePath_symlink_into_failover(roots, calibrationRoot):
    home, prod, _ = roots
    (prod / "Powder").mkdir()
    (home / "Powder").symlink_to(prod / "Powder")
    with pytest.raises(CalibrationWriteGuardError, match="outside of the calibration home"):
        calibrationRoot.writePath("Powder/LiteGroupMap.hdf")


def test_writePath_symlink_outside_home(roots, tmp_path, calibrationRoot):
    home, _, _ = roots
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    (home / "Powder").symlink_to(elsewhere)
    with pytest.raises(CalibrationWriteGuardError, match="outside of the calibration home"):
        calibrationRoot.writePath("Powder/LiteGroupMap.hdf")


def test_writePath_symlink_within_home(roots, calibrationRoot):
    home, _, _ = roots
    (home / "real").mkdir()
    (home / "Powder").symlink_to(home / "real")
    assert calibrationRoot.writePath("Powder/a.json") == home / "Powder" / "a.json"


def test_writePath_symlinked_home(roots, tmp_path):
    # the home itself may be a symlink
    home, prod, _ = roots
    link = tmp_path / "homeLink"
    link.symlink_to(home)
    root = CalibrationRoot(link, [prod], environment="qa", schemaVersion=SCHEMA)
    assert root.writePath("a.json") == link / "a.json"


@pytest.mark.usefixtures("mockLogger")
def test_writePath_protects_skipped_failover(roots, tmp_path):
    # a failover skipped as invalid is still protected
    home, _, _ = roots
    unstamped = tmp_path / "unstamped"
    unstamped.mkdir()
    root = CalibrationRoot(home, [unstamped], environment="qa", schemaVersion=SCHEMA)
    assert root.failovers == []
    (home / "Powder").symlink_to(unstamped)
    with pytest.raises(CalibrationWriteGuardError):
        root.writePath("Powder/a.json")


def test_writePath_userMode_home_stamped_for_another_environment(tmp_path):
    # e.g. a user yml pointing the home of a QA deployment at prod
    stamp(tmp_path, "prod")
    root = CalibrationRoot(tmp_path, environment="qa", schemaVersion=SCHEMA, userMode=True)
    with pytest.raises(CalibrationWriteGuardError, match="stamped for the 'prod' environment"):
        root.writePath("a.json")
    # reads are still allowed
    touch(tmp_path / "a.json")
    assert root.resolve("a.json") == tmp_path / "a.json"


def test_writePath_userMode_home_stamped_for_same_environment(tmp_path):
    stamp(tmp_path, "qa")
    root = CalibrationRoot(tmp_path, environment="qa", schemaVersion=SCHEMA, userMode=True)
    assert root.writePath("a.json") == tmp_path / "a.json"


def test_writePath_userMode_unstamped_home(tmp_path):
    root = CalibrationRoot(tmp_path, environment="qa", schemaVersion=SCHEMA, userMode=True)
    assert root.writePath("a.json") == tmp_path / "a.json"


def test_writes_never_land_in_failover(roots, calibrationRoot):
    # end-to-end: resolve, then write through the guard, then re-resolve
    home, prod, next_ = roots
    touch(prod / "groupingMap.json", "prod")
    assert calibrationRoot.resolve("groupingMap.json") == prod / "groupingMap.json"
    calibrationRoot.writePath("groupingMap.json").write_text("qa")
    assert (prod / "groupingMap.json").read_text() == "prod"
    assert calibrationRoot.resolve("groupingMap.json") == home / "groupingMap.json"
    assert not (next_ / "groupingMap.json").exists()
