import os
from glob import glob

moduleDir = os.path.dirname(os.path.abspath(__file__))
modules = glob(f"{moduleDir}/*.py")
all_module_names = {module[:-3].split("/")[-1] for module in modules if not module.endswith("__init__.py")}


def loadModule(x):
    import importlib

    from mantid.api import AlgorithmFactory
    from mantid.simpleapi import _create_algorithm_function

    for i in range(len(all_module_names)):
        try:
            module = importlib.import_module(f"{__name__}.{x}", x)
            break
        except ImportError as e:
            y = str(e).split("'")[1]
            if y in all_module_names:
                loadModule(y)
            else:
                raise e
            continue

    # get just the class name
    x = x.split(".")[-1]

    # NOTE all algorithms must have same class name as filename
    algoClass = getattr(module, x)
    AlgorithmFactory.subscribe(algoClass)
    algo = algoClass()
    algo.initialize()
    _create_algorithm_function(x, 1, algo)


for x in all_module_names:
    if x == "MantidSnapper":
        # MantidSnapper lives in this folder, but is not an algorithm
        continue
    if x == "CustomGroupWorkspace":
        # this algorithm has some issue and can't be created
        # but it also is dead code and never used
        continue

    if x == "PixelDiffractionCalibration":
        # this is actually a recipe being temporarily stored here
        # move it to recipe folder and delete this once review passes
        continue

    loadModule(x)

# cleanup
del glob
del os
