import importlib

from snapred import pullAllModules

__all__ = []
for moduleName in pullAllModules(__file__):
    module = importlib.import_module(f"{__name__}.{moduleName}", moduleName)
    if module.__dict__.get("__all__", None) is not None:
        __all__.extend(module.__dict__["__all__"])
    locals().update({moduleName: module.__dict__[moduleName]})

del pullAllModules
