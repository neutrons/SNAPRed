from typing import Any

from mantid.api import AlgorithmFactory, AlgorithmProperty, PythonAlgorithm
from mantid.kernel import Property


class StayResidentWrapper(PythonAlgorithm):
    """
    Allow an algorithm to be managed by `AlgorithmManager`, while controlling its persistence.
    """

    # Implementation notes:
    # * This wrapper algorithm is managed by `AlgorithmManager`, in order that
    #   any cleanup operations can be completed for its child algorithm at application exit.
    # * `StayResidentWrapper.isRunning()` returns `True` until the wrapper is cancelled.

    def category(self):
        return "SNAPRed Internal"

    def isRunning(self) -> bool:
        # This returns `True` until `cancel` is called,
        #   and thereby keeps the algorithm from being deleted by the `AlgorithmManager`.
        # (See `mantid/Framework/PythonInterface/mantid/api/src/PythonAlgorithm/AlgorithmAdapter.cpp`.)
        return self._isRunning

    def cancel(self):
        # Cancel this algorithm and its child -- this triggers garbage collection of the algorithm instances.
        # (See `mantid/Framework/PythonInterface/mantid/api/src/PythonAlgorithm/AlgorithmAdapter.cpp`.)
        child = self.getProperty("ChildAlgorithm")
        child.cancel()
        self._isRunning = False
        # Beyond this point, `self.isRunning()` will always return `False`.
        #   This instance, and it's child will then be deleted by the `AlgorithmManager`.

    def setProperty(self, key: str, value: Any):
        # Pass-through to the `<child>.setProperty`.
        child = self.getProperty("ChildAlgorithm")
        child.setProperty(key, value)

    def getProperty(self, key: str) -> Property:
        # Pass-through to the `<child>.getProperty`.
        child = self.getProperty("ChildAlgorithm")
        return child.getProperty(key)

    def PyInit(self):
        self.declareProperty(AlgorithmProperty("ChildAlgorithm"))

        # For the `StayResidentWrapper` itself: `_isRunning` functions in "one shot" mode:
        #   it should not be reset after the algorithm is cancelled.
        self._isRunning = True

    def PyExec(self):
        # [Re-]execute the wrapped algorithm,
        #   provided it has completed its last execution.

        child = self.getProperty("ChildAlgorithm")
        if child.isRunning():
            # It may be OK if the child is still running. Don't hammer it!
            return
        child.executeAsynch()


# Register algorithm with Mantid
AlgorithmFactory.subscribe(StayResidentWrapper)
