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


def access_pointer(pointer: int) -> Any:
    thing = ctypes.cast(pointer, ctypes.py_object).value
    if pointer in OBJCACHE:
        del OBJCACHE[pointer]
    return thing
