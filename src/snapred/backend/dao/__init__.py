from snapred import _pullAllModules, pullModuleMembers

# Pull members from current package modules, respecting __all__.
__all__, localz = pullModuleMembers(__file__, __name__)
# update locals such that module members can be accessed directly
locals().update(localz)


def reload():
    import importlib
    import sys

    for moduleName in _pullAllModules(__file__):
        module = importlib.import_module(f"{__name__}.{moduleName}", moduleName)
        if module.__name__ in sys.modules:
            importlib.reload(module)
