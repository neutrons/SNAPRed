from mantid.simpleapi import _create_algorithm_function

from snapred import pullModuleMembers

# Pull members from current package modules, respecting __all__.
__all__, localz = pullModuleMembers(__file__, __name__)

for x in __all__:
    if x == "MantidSnapper":
        # MantidSnapper lives in this folder, but is not an algorithm
        continue
    if x == "CustomGroupWorkspace":
        # this algorithm has some issue and can't be created
        # but it also is dead code and never used
        continue

    # NOTE all algorithms must have same class name as filename
    algoClass = getattr(locals()[x], x)
    algo = algoClass()
    algo.initialize()
    _create_algorithm_function(x, 1, algo)

# cleanup
del _create_algorithm_function
del pullModuleMembers
