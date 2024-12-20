import ctypes
from typing import Any

OBJCACHE = {}


def create_pointer(thing: Any) -> int:
    """
    Returns a memory address to be used as a pointer, and retains a handle to the object
    to ensure the memory can be accessed later.
    @param the object whose pointer is required
    @return the pointer to the object
    """
    OBJCACHE[id(thing)] = thing
    return id(thing)


def inspect_pointer(pointer: int) -> Any:
    """
    Fetch an object referenced by the pointer, without removing it from the cache.
    Useful for validateInputs with a pointer property.
    @param the pointer to the object
    @return the object pointed to
    """
    if pointer in OBJCACHE:
        return ctypes.cast(pointer, ctypes.py_object).value
    else:
        raise RuntimeError(f"No appropriate object held at address {hex(pointer)}")


def access_pointer(pointer: int) -> Any:
    """
    Fetch an objected referenced by the pointer, and remove it from the cache.
    @param the pointer to the object
    @return the object pointed to
    """
    thing = inspect_pointer(pointer)
    del OBJCACHE[pointer]
    return thing
