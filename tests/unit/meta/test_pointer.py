from unittest.mock import sentinel

from snapred.meta.pointer import access_pointer, create_pointer


def test_create_and_access_pointer():
    a = sentinel.some_object
    pa = create_pointer(a)
    aa = access_pointer(pa)
    assert a == aa


def test_pointer_persistence():
    a = {"one": 2, 3: "four"}
    pa = create_pointer(a)
    del a
    aa = access_pointer(pa)
    assert create_pointer(aa) == pa
