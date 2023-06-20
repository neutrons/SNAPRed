# import pytest
from snapred.meta.Config import Config, Resource


def test_environment():
    assert Config["environment"] == "test"


def test_instrument_home():
    correctPath = "/SNAPRed/tests/resources/SNAP/"
    assert Config["instrument.home"][-len(correctPath) :] == correctPath


def test_resource_exists():
    assert Resource.exists("application.yml")


def test_resource_not_exists():
    assert not Resource.exists("not_a_real_file.yml")


def test_resource_read():
    assert Resource.read("application.yml") is not None


def test_resouce_open():
    with Resource.open("application.yml", "r") as file:
        assert file is not None
