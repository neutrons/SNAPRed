from mantid.kernel import *
from mantid.api import *
import time

class DummyAlgo(PythonAlgorithm):

    def PyInit(self):
        # declare properties
        pass

    def PyExec(self):
        # run the algo
        self.log().notice("exec dummy Algo")
        endrange= 5

        prog_reporter = Progress(self, start=0.0, end=1.0, nreports=endrange)
        for i in range(0, endrange):
            time.sleep(1)
            prog_reporter.reportIncrement(i, "processing...")

        prog_reporter.report(endrange, "Done")

# Register algorithm with Mantid
AlgorithmFactory.subscribe(DummyAlgo)