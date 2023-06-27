def delegate(cls, attr_name, method_name):
    def delegated(self, *vargs, **kwargs):
        a = getattr(self, attr_name)
        _set = getattr(self, "_set")
        if not _set:
            raise AttributeError("Callback not Populated")
        m = getattr(a, method_name)
        return m(*vargs, **kwargs)

    setattr(cls, method_name, delegated)


def callback(clazz):
    ignore = [
        "_ignore",
        "update",
        "_set",
        "get",
        "__class__",
        "_value",
        "__getitem__",
        "__new__",
        "__init__",
        "__getattr__",
        "getattr",
        "__getattribute__",
        "__setattr__",
        "__subclasscheck__",
    ]

    class Callback(object):
        _ignore = ignore
        _set = False
        _value: clazz = None

        def __init__(self):
            pass

        def update(self, value):
            self._set = True
            self._value = value

        def get(self):
            if not self._set:
                raise AttributeError("Callback not Populated")
            return self._value

        def __getattr__(self, name):
            if name in self._ignore:
                return __getattr__(name)  # noqa: F821
            if not self._set:
                raise AttributeError("Callback not Populated")
            return getattr(self._value, name)

        def __getitem__(self, items):
            if not self._set:
                return self
            return self._value.__getitem__(items)

        def __subclasscheck__(cls, subclass):
            return clazz.__subclasscheck__(subclass)

    # Forwared all methods to the _value, throw if not populated
    for name in dir(clazz):
        if name not in ignore:
            delegate(Callback, "_value", name)

    return Callback()
