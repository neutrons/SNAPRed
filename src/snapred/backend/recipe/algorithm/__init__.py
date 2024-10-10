import importlib
import os
from glob import glob

from mantid.api import AlgorithmFactory
from mantid.simpleapi import _create_algorithm_function

moduleDir = os.path.dirname(os.path.abspath(__file__))
modules = glob(f"{moduleDir}/*.py")
all_module_names = [module[:-3].split("/")[-1] for module in modules if not module.endswith("__init__.py")]

for x in all_module_names:
    if x == "MantidSnapper":
        # MantidSnapper lives in this folder, but is not an algorithm
        continue
    if x == "CustomGroupWorkspace":
        # this algorithm has some issue and can't be created
        # but it also is dead code and never used
        continue

    while True:
        try:
            module = importlib.import_module(f"{__name__}.{x}", x)
            break
        except ImportError as e:
            print(f"{x} : {e.name}")
            if e.name in all_module_names:
                import e.name
            break

    # for the leftovers algos, this gets just the class name
    x = x.split(".")[-1]

    # NOTE all algorithms must have same class name as filename
    algoClass = getattr(module, x)
    AlgorithmFactory.subscribe(algoClass)
    algo = algoClass()
    algo.initialize()
    _create_algorithm_function(x, 1, algo)

# cleanup
del _create_algorithm_function
del AlgorithmFactory
del glob
del os
del importlib
