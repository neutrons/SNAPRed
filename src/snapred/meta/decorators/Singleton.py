from functools import wraps


def Singleton(orig_cls):
    orig_new = orig_cls.__new__
    instance = None

    @wraps(orig_cls.__init__)
    def __init__(self, *args, **kwargs):
        if instance is not None:
            return
        orig_cls.__init__(self, *args, **kwargs)

    @wraps(orig_cls.__new__)
    def __new__(cls, *args, **kwargs):
        nonlocal instance
        if instance is None:
            instance = orig_new(cls, *args, **kwargs)
        return instance

    orig_cls.__new__ = __new__
    return orig_cls
