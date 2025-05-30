import ctypes
import gc

import pytest

from snapred.meta.pointer import access_pointer, create_pointer, inspect_pointer


def make_a():
    """Create an object that will be accessed by pointer"""
    return {"one": 2, 3: "four"}


def called_func1():
    """Return the memory address of a temporary object by id"""
    return id(make_a())


def called_func2():
    """Return the memory object of a temporary object using create_pointer"""
    return create_pointer(make_a())


def test_create_and_access_pointer():
    """Ensure reflexive property"""
    a = make_a()
    pa = create_pointer(a)
    aa = access_pointer(pa)
    assert a == aa


def test_pointer_persistence():
    """Ensure pointers accessed in this way are safe from garbage collection"""
    # if accessed only through id, this can fail
    pa = called_func1()
    gc.collect()
    aa = ctypes.cast(pa, ctypes.py_object)
    assert aa.__dict__ == {}
    # NOTE trying to access aa any further will create a segfault

    # if accessed through these methods, safe from garbage collection
    pa = called_func2()
    gc.collect()
    aa = access_pointer(pa)
    assert aa.__dir__() != {}
    assert aa == make_a()


def test_inspect_pointer():
    """Ensure pointers can be expected and still remain in the queue"""
    a = make_a()
    pa = create_pointer(a)
    aa = inspect_pointer(pa)
    aaa = access_pointer(pa)
    assert a == aa
    assert a == aaa


def test_inspect_bad_pointer_error():
    """Ensure accessing a bad pointer raises an error"""

    # create a pointer then access it, which removes it from the cache
    a = make_a()
    pa = create_pointer(a)
    access_pointer(pa)

    with pytest.raises(RuntimeError):
        inspect_pointer(pa)
