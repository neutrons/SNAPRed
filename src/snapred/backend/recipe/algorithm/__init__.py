from mantid.api import PythonAlgorithm
from mantid.simpleapi import _create_algorithm_function

from snapred import pullModuleMembers

# Pull members from current package modules, respecting __all__.
__all__, localz = pullModuleMembers(__file__, __name__)
# update locals such that module members can be accessed directly
locals().update(localz)

for x in __all__:
    if not isinstance(x, PythonAlgorithm):
        # MantidSnapper is not an algorithm, but
        # lives in the algorithm folder
        continue
    if x == "CustomGroupWorkspace":
        # this algorithm has some issue and can't be created
        # but it also is dead code and never used
        continue

    algoClass = globals()[x]
    algo = algoClass()
    algo.initialize()
    _create_algorithm_function(x, 1, algo)

# cleanup
del _create_algorithm_function
del pullModuleMembers
