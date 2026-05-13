import unittest

import pytest

from snapred.meta.Callback import Callback, callback


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

    def test_getattr_nameInIgnore(self):
        # When `__getattr__` is invoked with a name that is in `self._ignore`,
        # the implementation falls through to the Callback object.
        testCallback = callback(str)
        assert testCallback.__getattr__("_ignore") == Callback._ignore

    def test_setattr_notPopulated(self):
        # Setting an attribute when the callback is not populated should
        # succeed (the class does not override `__setattr__`) and must not
        # mark the callback as populated.
        testCallback = callback(str)
        assert testCallback._set is False
        testCallback.someNewAttribute = "hello"
        assert testCallback.someNewAttribute == "hello"
        # Setting an arbitrary attribute must not flip the populated flag.
        assert testCallback._set is False
        # `get` should still raise because the callback was never updated.
        with pytest.raises(AttributeError):
            testCallback.get()

    def test_repr_notPopulated(self):
        testCallback = callback(str)
        repr_ = repr(testCallback)
        assert "Callback(str" in repr_
        assert "not populated" in repr_

    def test_repr_populated(self):
        testCallback = callback(str)
        testCallback.update("test")
        repr_ = repr(testCallback)
        assert "Callback(str" in repr_
        assert f"value={'test'!r}" in repr_

    def test_str_notPopulated(self):
        testCallback = callback(str)
        str_ = str(testCallback)
        assert "<Callback(str)>" == str_

    def test_str_populated(self):
        testCallback = callback(str)
        testCallback.update("test")
        str_ = str(testCallback)
        assert str_ == "test"

    def test_callbackInstances(self):
        # `callback()` returns distinct instances, but the underlying class is
        # cached per wrapped type.
        cb1 = callback(int)
        cb2 = callback(int)
        cb3 = callback(str)

        # Distinct instances ...
        assert cb1 is not cb2
        # ... but a single cached class per wrapped type ...
        assert cb1.__class__ is cb2.__class__
        # ... and a distinct class for each distinct wrapped type.
        assert cb1.__class__ is not cb3.__class__

        # Generated class names encode the wrapped type.
        assert cb1.__class__.__name__ == "Callback[int]"
        assert cb3.__class__.__name__ == "Callback[str]"

        # Independent state: updating one instance does not affect the other.
        cb1.update(10)
        cb2.update(20)
        assert cb1.get() == 10
        assert cb2.get() == 20

        # Forwarded magic methods continue to work for each instance.
        assert cb1 + 5 == 15
        assert cb2 + 5 == 25

    def test_strSet(self):
        # Once populated, `str()` is forwarded to the wrapped value.
        testCallback = callback(str)
        testCallback.update("test")
        assert str(testCallback) == "test"

    def test_listCallback(self):
        testCallback = callback(list)
        testCallback.update([1, 2, 3])
        assert testCallback.get() == [1, 2, 3]
        assert len(testCallback) == 3
        assert testCallback[1] == 2
        assert 2 in testCallback
        assert list(iter(testCallback)) == [1, 2, 3]

    def test_setItemForwarded(self):
        testCallback = callback(list)
        testCallback.update([1, 2, 3])
        testCallback[0] = 99
        assert testCallback.get() == [99, 2, 3]

    def test_setAttrForwardedWhenSet(self):
        # When populated, setting a non-internal attribute is forwarded to the
        # wrapped value.
        class _Bag:
            pass

        bag = _Bag()
        testCallback = callback(_Bag)
        testCallback.update(bag)
        testCallback.name = "forwarded"
        assert bag.name == "forwarded"

    def test_unsetMagicMethodThrows(self):
        # A representative forwarded magic method should raise when the
        # callback is not populated.
        testCallback = callback(int)
        with pytest.raises(AttributeError):
            testCallback + 1
        with pytest.raises(AttributeError):
            len(callback(list))
