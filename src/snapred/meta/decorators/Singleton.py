from functools import wraps
from typing import List

_Singleton_instances: List[type] = []


def Singleton(orig_cls):
    orig_new = orig_cls.__new__
    orig_init = orig_cls.__init__
    instance = None
    initialized = False

    @wraps(orig_cls.__init__)
    def __init__(self, *args, **kwargs):
        nonlocal initialized
        if initialized:
            return
        initialized = True
        orig_init(self, *args, **kwargs)

    @wraps(orig_cls.__new__)
    def __new__(cls, *args, **kwargs): # noqa: ARG001
        nonlocal instance
        if instance is None:
            # this needs to work with object.__new__, which only has only the `cls` arg
            instance = orig_new(cls)  # , *args, **kwargs)
        return instance

    def _reset_Singleton(*, fully_unwrap: bool = False):
        # Reset the Singleton:
        #
        #   * The `@Singleton` decorator is applied at time of import.
        #   * This method does not _reinitialize_ any existing class instances;
        #       it just ensures that the next time the `__init__` is called it will
        #       initialize a new instance.
        #   * If `fully_unwrap` is set, it is equivalent to removing the `@Singleton` decorator from the class.
        #
        #   This method is provided for use by `pytest` fixtures,
        #     as an alternative to mocking out the `Singleton` entirely.
        #
        nonlocal instance
        nonlocal initialized
        instance = None
        initialized = False

        if fully_unwrap:
            # this should be equivalent to mocking out the decorator
            orig_cls.__new__ = orig_cls.__new__.__wrapped__
            orig_cls.__init__ = orig_cls.__init__.__wrapped__

    orig_cls.__new__ = __new__
    orig_cls.__init__ = __init__

    orig_cls._reset_Singleton = _reset_Singleton
    global _Singleton_instances
    _Singleton_instances.append(orig_cls)

    return orig_cls


def reset_Singletons(*, fully_unwrap: bool = False):
    # Implementation notes:
    #
    #   * The `@Singleton` decorator is applied at time of import.
    #   * This module-scope method does not _reinitialize_ any existing class instances;
    #       it just ensures that the next time the `__init__`[s] are called they will
    #       initialize new instances.
    #   * If `fully_unwrap` is set, it is equivalent to removing the `@Singleton` decorator from all of the classes;
    #       and, if that approach is taken, it's only necessary to apply this method at `scope="session"`.
    #
    global _Singleton_instances
    for cls in _Singleton_instances:
        cls._reset_Singleton(fully_unwrap=fully_unwrap)
