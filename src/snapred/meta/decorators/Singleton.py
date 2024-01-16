from functools import wraps


def Singleton(orig_cls):
    orig_new = orig_cls.__new__
    orig_init = orig_cls.__init__
    instance = None
    initialized = False

    @wraps(orig_cls.__init__)
    def __init__(self, *args, **kwargs):
        nonlocal initialized
        if initialized is True:
            return
        initialized = True
        orig_init(self, *args, **kwargs)

    @wraps(orig_cls.__new__)
    def __new__(cls, *args, **kwargs):
        nonlocal instance
        if instance is None:
            instance = orig_new(cls, *args, **kwargs)
        return instance

    orig_cls.__new__ = __new__
    orig_cls.__init__ = __init__
    return orig_cls
