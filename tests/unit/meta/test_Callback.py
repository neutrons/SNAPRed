import unittest

import pytest

from snapred.meta.Callback import callback


class TestCallback(unittest.TestCase):
    def test_unsetThrows(self):
        testCallback = callback(str)
        with pytest.raises(AttributeError):
            testCallback.upper()
        with pytest.raises(AttributeError):
            testCallback == "test"

    def test_stringCallback(self):
        testCallback = callback(str)
        testCallback.update("test")
        assert testCallback.get() == "test"
        assert testCallback.upper() == "TEST"
        assert testCallback == "test"
        assert testCallback[0] == "t"

    def test_updateCallback(self):
        testCallback = callback(str)
        with pytest.raises(AttributeError):
            testCallback.get()
        testCallback.update("test")
        assert testCallback.get() == "test"

    def test_boolCallback(self):
        testCallback = callback(bool)
        testCallback.update(True)
        assert testCallback.get() is True
        assert testCallback.__bool__() is True
        assert testCallback

    def test_intCallback(self):
        testCallback = callback(int)
        testCallback.update(123)
        assert testCallback.get() == 123
        assert testCallback.__int__() == 123
        assert testCallback == 123
        assert testCallback + 1 == 124

    def test_floatCallback(self):
        testCallback = callback(float)
        testCallback.update(123.456)
        assert testCallback.get() == 123.456
        assert testCallback.__float__() == 123.456
        assert testCallback == 123.456
        assert testCallback + 1 == 124.456
