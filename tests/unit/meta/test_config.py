# import pytest
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
