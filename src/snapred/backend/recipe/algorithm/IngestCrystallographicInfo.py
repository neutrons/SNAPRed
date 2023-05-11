from mantid.kernel import *
from mantid.api import AlgorithmFactory, PythonAlgorithm
import time

from snapred.backend.dao.CrystallographicInfo import CrystallographicInfo

name = "IngestCrystallographicInfo"

class IngestCrystallographicInfo(PythonAlgorithm):

    def PyInit(self):
        # declare properties
        pass

    def PyExec(self):
        # run the algo
        self.log().notice("ingest crystallogtaphic info")
        endrange= 5

        prog_reporter = Progress(self, start=0.0, end=1.0, nreports=endrange)
        for i in range(0, endrange):
            time.sleep(1)
            prog_reporter.reportIncrement(i, "processing...")

        prog_reporter.report(endrange, "Done")


    def findWeakFSquared(self, xtal: CrystallographicInfo):
        print("HELLOW THERE")

        I0 = [ff * mm for ff, mm in zip(xtal.fSquared, xtal.multiplicities)]
        I0.sort()

        numPeaks = len(xtal.fSquared)
        lowest = max(1,round(numPeaks/100))-1

        print(f"{numPeaks} : {lowest}")

        return I0[int(lowest/2)]



# Register algorithm with Mantid
AlgorithmFactory.subscribe(IngestCrystallographicInfo)
