import pytest
from snapred.meta.Config import Config, Resource, _find_root_dir


def test_environment():
    assert Config["environment"] == "test"


def test_find_root_dir():
    assert _find_root_dir().endswith("/tests")


def test_instrument_home():
    # test verifies that the end of the path is correct
    correctPathEnding = "/tests/resources/SNAP/"
    assert Config["instrument.home"].endswith(correctPathEnding)


def test_resource_exists():
    assert Resource.exists("application.yml")


def test_resource_not_exists():
    assert not Resource.exists("not_a_real_file.yml")


def test_resource_read():
    assert Resource.read("application.yml") is not None


def test_resouce_open():
    with Resource.open("application.yml", "r") as file:
        assert file is not None


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
