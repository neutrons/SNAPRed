"""SNAPRed: a Python package for reduction and caliration of the SNAPRed instrument"""
try:
    from ._version import __version__  # noqa: F401
except ImportError:
    __version__ = "unknown"


def _pullAllModules(file=__file__):
    """
    Grabs all the modules in the package.
    """
    import os
    from glob import glob

    # TODO: Does this work in package mode?
    moduleDir = os.path.dirname(os.path.abspath(file))
    # Get list of *.py files
    modules = glob(f"{moduleDir}/*.py")
    # Import all modules
    return [module[:-3].split("/")[-1] for module in modules if not module.endswith("__init__.py")]


def pullModuleMembers(file=__file__, name=__name__):
    """
    Does a shallow pull of all the modules in the package.
    Returns a flattened list of all the __all__ lists in the modules.
    Returns a dict of used to update the local namespace of the caller.
    (This enables `from snapred.dao import Data` instead of `from snapred.dao.Data import Data`)
    """
    import importlib

    allz = []
    localz = {}
    for moduleName in _pullAllModules(file):
        module = importlib.import_module(f"{name}.{moduleName}", moduleName)
        if module.__dict__.get("__all__", None) is not None:
            allz.extend(module.__dict__["__all__"])
            # update locals with all the names in __all__
            localz.update({key: module.__dict__[key] for key in module.__dict__["__all__"]})
        else:
            # this assumes the FileName=ClassName pattern
            allz.append(moduleName)
            localz.update({moduleName: module.__dict__[moduleName]})
            # append module to __all__ prepened with _
            allz.append(f"_{moduleName}")
            localz.update({f"_{moduleName}": module})
    return allz, localz
