"""SNAPRed: a Python package for reduction and caliration of the SNAPRed instrument"""
try:
    from ._version import __version__  # noqa: F401
except ImportError:
    __version__ = "unknown"


def pullAllModules(file=__file__):
    import os
    from glob import glob

    # TODO: Does this work in package mode?
    moduleDir = os.path.dirname(os.path.abspath(file))
    # Get list of *.py files
    modules = glob(f"{moduleDir}/*.py")
    return [module[:-3].split("/")[-1] for module in modules if not module.endswith("__init__.py")]


__all__ = pullAllModules()
