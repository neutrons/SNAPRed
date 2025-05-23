import logging
import os
import shutil
import sys
from pathlib import Path
from tempfile import TemporaryDirectory

##
## In order to preserve the normal import order as much as possible,
##   place test-specific imports last.
##
from unittest import mock

import pytest
import yaml

import snapred.meta.Config as Config_module
from snapred.backend.log.logger import snapredLogger
from snapred.meta.Config import (
    Config,
    Resource,
    _find_root_dir,
    _pythonLoggingLevelFromMantid,
    _pythonLoggingLevelFromString,
    fromMantidLoggingLevel,
    fromPythonLoggingLevel,
)


def test_environment():
    assert Config["environment"] == "test"


def test_find_root_dir_test_env():
    # Test that a test environment's `MODULE_ROOT` is set to the `tests` directory.
    assert _find_root_dir().endswith("/tests")


def test_version_default():
    # Test that Config["version.default"] is implicitly set
    assert isinstance(Config["version.default"], int)


@mock.patch.dict(os.environ, values={"env": "dev"}, clear=True)
def test_find_root_dir_non_test_env():
    # Test that a non-test environment's `MODULE_ROOT` is set to the SNAPRed module directory
    assert Path(_find_root_dir()) == Path(sys.modules["snapred"].__file__).parent


@mock.patch.dict(os.environ, values={"env": "dev_test"}, clear=True)
def test_find_root_dir_special_test_env():
    # Test that a special test environment's (any "env" with "test" in the name)
    #   `MODULE_ROOT` is set to the `tests` directory.
    assert _find_root_dir().endswith("/tests")


@mock.patch.dict(os.environ, values={"env": "dev"}, clear=True)
@mock.patch.dict(sys.modules, clear=True)
def test_find_root_dir_failure():
    # Test that not being able to define the `MODULE_ROOT` raises an exception.
    with pytest.raises(Exception, match="Unable to determine SNAPRed module-root directory"):
        _find_root_dir()


def test_instrument_home():
    # test verifies that the end of the path is correct
    correctPathEnding = "/tests/resources/inputs"
    assert Config["instrument.home"].endswith(correctPathEnding)


def test_resource_packageMode(caplog):
    # Test that "package mode" is recognized appropriately.

    # TODO: At present, 'Config' has a _redundant_ '@Singleton' =>  It is also initialized
    #   explicitly as a singleton.  This needs to be fixed!

    ROOT_MODULE = Path(sys.modules["snapred"].__file__).parent
    ymlPath = ROOT_MODULE / "resources" / "application.yml"

    # In the below, we need to trigger a fresh import for the 'Config' module.
    # AND, we need an absolute path for an "application.yml" which is _outside_ of "snapred/resources".
    with (
        mock.patch.dict(os.environ),
        mock.patch.dict(sys.modules),
        TemporaryDirectory() as tmpdir,
    ):
        # An absolute path for "application.yml" _outside_ of "snapred/resources".
        nonModuleEnvPath = Path(tmpdir) / "application.yml"
        shutil.copy2(ymlPath, nonModuleEnvPath)
        os.environ["env"] = str(nonModuleEnvPath)

        # Trigger a fresh import for the "Config" module.
        del sys.modules["snapred.meta.Config"]
        from snapred.meta.Config import _Resource

        # `@Singleton` is now active for tests:
        #    we need to reset it, so that we can recreate the class.
        # In this case, we need to fully remove the decorator, so that the original `__init__` will be called.
        # Otherwise, the applied mocks will have no effect during the initialization.
        _Resource._reset_Singleton(fully_unwrap=True)

        with (
            mock.patch.object(_Resource, "_existsInPackage") as mockExistsInPackage,
            caplog.at_level(logging.DEBUG, logger="snapred.meta.Config.Resource"),
        ):
            # This mock bypasses the fact that "application.yml" actually does exist
            #   under "snapred/resources".  Probably there's a better way to do this!
            mockExistsInPackage.return_value = False
            rs = _Resource()
            assert rs._packageMode
        assert "In package mode" in caplog.text


def test_resource_not_packageMode(caplog):
    # Test that a test env is recognized as non-"package mode".

    with mock.patch.dict(sys.modules):
        # Trigger a fresh import for the "Config" module.
        del sys.modules["snapred.meta.Config"]
        with caplog.at_level(logging.DEBUG, logger="snapred.meta.Config.Resource"):
            from snapred.meta.Config import _Resource

            rs = _Resource()
            assert not rs._packageMode
    assert "Not in package mode" in caplog.text


def test_resource_packageMode_exists():
    # Test that the "exists" method in package mode implements <exists in the package> functionality.

    ROOT_MODULE = Path(sys.modules["snapred"].__file__).parent
    ymlPath = ROOT_MODULE / "resources" / "application.yml"

    # In the below, we need to trigger a fresh import for the 'Config' module.
    # AND, we need an absolute path for an "application.yml" which is _outside_ of "snapred/resources".
    with (
        mock.patch.dict(os.environ),
        mock.patch.dict(sys.modules),
        TemporaryDirectory() as tmpdir,
    ):
        # An absolute path for "application.yml" _outside_ of "snapred/resources".
        nonModuleEnvPath = Path(tmpdir) / "application.yml"
        shutil.copy2(ymlPath, nonModuleEnvPath)
        os.environ["env"] = str(nonModuleEnvPath)

        # Trigger a fresh import for the "Config" module.
        del sys.modules["snapred.meta.Config"]
        from snapred.meta.Config import _Resource

        # `@Singleton` is now active for tests:
        #    we need to reset it, so that we can recreate the class.
        # In this case, we need to fully remove the decorator, so that the original `__init__` will be called.
        # Otherwise, the applied mocks will have no effect during the initialization.
        _Resource._reset_Singleton(fully_unwrap=True)

        with (
            mock.patch.object(_Resource, "_existsInPackage") as mockExistsInPackage,
        ):
            # This mock bypasses the fact that "application.yml" actually does exist
            #   under "snapred/resources".  Probably there's a better way to do this!
            mockExistsInPackage.return_value = False
            rs = _Resource()
            assert rs._packageMode
            test_path = "any/path"
            rs.exists(test_path)
            mockExistsInPackage.assert_called_with(test_path)


def test_resource_exists():
    with mock.patch.object(Resource, "_existsInPackage") as mockExistsInPackage:
        assert Resource.exists("application.yml")
        mockExistsInPackage.assert_not_called()


def test_resource_exists_false():
    assert not Resource.exists("not_a_real_file.yml")


def test_resource_read():
    assert Resource.read("application.yml") is not None


def test_resource_open():
    with mock.patch.object(Config_module.resources, "path") as mockResourcesPath:
        assert not Resource._packageMode
        with Resource.open("application.yml", "r") as file:
            assert file is not None
            mockResourcesPath.assert_not_called()


def test_resource_packageMode_open():
    actual_path = Resource.getPath("application.yml")
    with (
        mock.patch.object(Config_module.resources, "path") as mockResourcesPath,
        mock.patch.object(Resource, "_packageMode") as mockPackageMode,
    ):
        mockResourcesPath.return_value = mock.Mock(
            __enter__=mock.Mock(return_value=actual_path),
            __exit__=mock.Mock(),
        )
        mockPackageMode.return_value = True
        test_path = "application.yml"
        with Resource.open(test_path, "r") as file:
            assert file is not None
            mockResourcesPath.assert_called_once_with("snapred.resources", test_path)


def test_Config_persistBackup():
    # mock Path.home() to temporary directory
    with TemporaryDirectory() as tmpdir:
        with mock.patch.object(Config_module._Config, "userApplicationDataHome", Path(tmpdir) / ".snapred"):
            inst = Config_module._Config()
            # remove the application.yml.bak file if it exists
            if (Path(tmpdir) / ".snapred" / "application.yml.bak").exists():
                os.remove(Path(tmpdir) / ".snapred" / "application.yml.bak")
                os.rmdir(Path(tmpdir) / ".snapred")

            assert not (Path(tmpdir) / ".snapred").exists()
            # call the persistBackup method
            inst.persistBackup()
            assert (Path(tmpdir) / ".snapred").exists()
            assert (Path(tmpdir) / ".snapred" / "application.yml.bak").exists()


def test_Config_accessor():
    # these values are copied from tests/resources/application.yml
    assert Config["environment"] == "test"
    assert Config["instrument.name"] == "SNAP"
    assert Config["nexus.file.extension"] == ".nxs.h5"
    assert Config["calibration.file.extension"] == ".json"

    # these should throw KeyError
    with pytest.raises(KeyError):
        assert Config["garbage"]
    with pytest.raises(KeyError):
        assert Config["orchestration.garbage"]


def test_key_substitution():
    testString = "This is a test string with a ${test.key} in it"
    Config._config["test"]["key"] = "value"
    Config._config["test"]["substitution"] = testString
    assert Config["test.substitution"] == "This is a test string with a value in it"


def test_multi_level_substitution():
    assert Config["test.data.home.write"] == f'~/{Config["test.config.home"]}/data/{Config["test.config.name"]}'
    assert Config["test.data.home.read"] == f'{Config["test.config.home"]}/data/{Config["test.config.name"]}'


def test_Config_validate(caplog):
    # `caplog` doesn't work with the `snapredLogger`.
    with mock.patch.object(snapredLogger, "getLogger", lambda name: logging.getLogger(name)):
        logging_SNAP_stream_format = Config["logging.SNAP.stream.format"]

        def mock__getitem__(key: str, liveDataFlag: bool, normalizeByBeamMonitorFlag: bool):
            match key:
                case "liveData.enabled":
                    return liveDataFlag
                case "mantid.workspace.normalizeByBeamMonitor":
                    return normalizeByBeamMonitorFlag
                case "logging.SNAP.stream.format":
                    return logging_SNAP_stream_format
                case _:
                    raise RuntimeError(f"unexpected key: {key}")

        # -------- WARNING if both are set: 'liveData.enabled' and 'mantid.workbench.normalizeByBeamMonitor' --------
        # Test positive case:
        with mock.patch.object(
            Config_module._Config, "__getitem__", lambda _self, key: mock__getitem__(key, True, True)
        ):
            assert Config["liveData.enabled"]
            assert Config["mantid.workspace.normalizeByBeamMonitor"]
            with caplog.at_level(logging.WARNING, logger="snapred.meta.Config.Config"):
                Config.validate()
            assert "'mantid.workspace.normalizeByBeamMonitor' and 'liveData.enabled'" in caplog.text
            caplog.clear()

        # Test negative cases:
        with mock.patch.object(
            Config_module._Config, "__getitem__", lambda _self, key: mock__getitem__(key, False, True)
        ):
            assert not Config["liveData.enabled"]
            assert Config["mantid.workspace.normalizeByBeamMonitor"]
            with caplog.at_level(logging.WARNING, logger="snapred.meta.Config.Config"):
                Config.validate()
            assert caplog.text == ""
            caplog.clear()

        with mock.patch.object(
            Config_module._Config, "__getitem__", lambda _self, key: mock__getitem__(key, True, False)
        ):
            assert Config["liveData.enabled"]
            assert not Config["mantid.workspace.normalizeByBeamMonitor"]
            with caplog.at_level(logging.WARNING, logger="snapred.meta.Config.Config"):
                Config.validate()
            assert caplog.text == ""
            caplog.clear()


def test_fromMantidLoggingLevel():
    for mantidLevel, pythonLevel in _pythonLoggingLevelFromMantid.items():
        assert pythonLevel == fromMantidLoggingLevel(mantidLevel)


def test_fromMantidLoggingLevel__uppercase():
    for mantidLevel, pythonLevel in _pythonLoggingLevelFromMantid.items():
        assert pythonLevel == fromMantidLoggingLevel(mantidLevel.upper())


def test_fromMantidLoggingLevel__unknown():
    with pytest.raises(RuntimeError, match=r".*can't convert.* to a Python logging level.*"):
        pythonLevel = fromMantidLoggingLevel("not_a_log_level")  # noqa: F841


def test_fromPythonLoggingLevel_int():
    mantidLoggingLevelFromPython_ = {v: k for k, v in _pythonLoggingLevelFromMantid.items()}
    for _, pythonLevel in _pythonLoggingLevelFromString.items():
        assert fromPythonLoggingLevel(pythonLevel) == mantidLoggingLevelFromPython_[pythonLevel]


def test_fromPythonLoggingLevel_str():
    mantidLoggingLevelFromPython_ = {v: k for k, v in _pythonLoggingLevelFromMantid.items()}
    for pythonLevelString, pythonLevel in _pythonLoggingLevelFromString.items():
        assert fromPythonLoggingLevel(pythonLevelString) == mantidLoggingLevelFromPython_[pythonLevel]


def test_fromPythonLoggingLevel__unknown_int():
    with pytest.raises(RuntimeError, match=r".*can't convert.* to a Mantid logging level.*"):
        pythonLevel = fromPythonLoggingLevel(1000)  # noqa: F841


def test_fromPythonLoggingLevel__unknown_str():
    with pytest.raises(RuntimeError, match=r".*can't convert.* to a Mantid logging level.*"):
        pythonLevel = fromPythonLoggingLevel("not a log level")  # noqa: F841


def test_swapToUserYml():
    # setup temp dir
    with TemporaryDirectory(prefix=Resource.getPath("outputs/")) as tmpPath:
        # mock out path's home method
        with mock.patch("snapred.meta.Config.Path.home") as mockHome:
            Config.snapredVersion = lambda: "1.0.0"
            mockHome.return_value = Path(tmpPath)
            Config.swapToUserYml()
            # check that the file exists
            assert Path(tmpPath).exists()
            assert (Path(tmpPath) / ".snapred").exists()
            assert (Path(tmpPath) / ".snapred" / "snapred-user.yml").exists()

            assert "snapred-user" in Config["environment"]

            with open(Path(tmpPath) / ".snapred" / "snapred-user.yml", "r") as file:
                yml = yaml.safe_load(file)
                assert yml["application"]["version"] == "1.0.0"
                assert yml["instrument"]["calibration"]["home"] is not None


def test_swapToUserYml_archive():
    # setup temp dir
    with TemporaryDirectory(prefix=Resource.getPath("outputs/")) as tmpPath:
        # mock out path's home method
        with (
            mock.patch("snapred.meta.Config.Path.home") as mockHome,
            mock.patch("snapred.meta.Config.datetime") as mockDateTime,
        ):
            dateTime = "2023-10-01 12:00:00"
            mockDateTime.datetime.now().strftime.return_value = dateTime
            Config.snapredVersion = lambda: "1.0.0"
            mockHome.return_value = Path(tmpPath)
            Config.swapToUserYml()
            # check that the file exists
            assert Path(tmpPath).exists()
            assert (Path(tmpPath) / ".snapred").exists()
            assert (Path(tmpPath) / ".snapred" / "snapred-user.yml").exists()

            assert "snapred-user" in Config["environment"]
            Config.snapredVersion = lambda: "1.0.8"
            Config.swapToUserYml()
            with open(Path(tmpPath) / ".snapred" / "snapred-user.yml", "r") as file:
                yml = yaml.safe_load(file)
                assert yml["application"]["version"] == "1.0.8"
                assert yml["instrument"]["calibration"]["home"] is not None
            assert (Path(tmpPath) / ".snapred" / "snapred-user.yml").exists()
            assert (Path(tmpPath) / ".snapred" / f"snapred-user-1.0.0-{dateTime}.yml.bak").exists(), os.listdir(
                Path(tmpPath) / ".snapred"
            )
