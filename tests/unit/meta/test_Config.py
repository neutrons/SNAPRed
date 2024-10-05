import logging
import os
import shutil
import sys
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

import pytest
import snapred.meta.Config as Config_module
from snapred.meta.Config import Config, Resource, _find_root_dir, python_log_level_from_Mantid_level


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


def test_config_accessor():
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


def test_python_log_level_from_Mantid_level():
    for level in ['none', 'fatal', 'critical', 'error', 'warning', 'notice', 'debug', 'trace']:
        pythonLevel = python_log_level_from_Mantid_level(level)
        assert isinstance(pythonLevel, int)
        assert 0 <= pythonLevel
        assert pythonLevel <= 100


def test_python_log_level_from_Mantid_level__uppercase():
    for level in ['none', 'fatal', 'critical', 'error', 'warning', 'notice', 'debug', 'trace']:
        pythonLevel = python_log_level_from_Mantid_level(level.upper())
        assert isinstance(pythonLevel, int)
        assert 0 <= pythonLevel
        assert pythonLevel <= 100


def test_python_log_level_from_Mantid_level__unknown():
    with pytest.raises(RuntimeError, match=r".*can't convert.* to a python\.logging level.*"):
        pythonLevel = python_log_level_from_Mantid_level("not_a_log_level") # noqa: F841
