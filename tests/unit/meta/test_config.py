# import pytest
from snapred.meta.Config import Config


def test_environment():
    assert Config["environment"] == "test"


def test_instrument_home():
    correctPath = "/SNAPred/tests/resources/SNAP/"
    assert Config["instrument.home"][-len(correctPath) :] == correctPath
