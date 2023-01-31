from mantid.kernel import *
from mantid.api import *
import time

from mantid.api import AlgorithmManager

name = "ReductionAlgorithm"

class ReductionAlgorithm(PythonAlgorithm):

    _endrange=11
    _progressCounter = 0
    _prog_reporter = Progress(self, start=0.0, end=1.0, nreports=_endrange)

    def PyInit(self):
        # declare properties
        self.declareProperty('ReductionIngredients', None)
        pass

    def createChildAlgorithm(self, name):
        alg = AlgorithmManager.create(name)
        alg.setChild(True)
        return alg

    def reportAndIncrement(self, _progressCounter, message):
        self._prog_reporter.reportIncrement(_progressCounter, message)
        self._progressCounter += 1
    
    def normalizeByCurrent(self):
        self.reportAndIncrement("Normalizing By Current...")
        normalizeByCurrent = self.createChildAlgorithm("NormalizeByCurrent")
        # set params
        normalizeByCurrent.execute()

    def applyDiffCal(self):
        self.reportAndIncrement("Applying DiffCal...")
        applyDiffCal = self.createChildAlgorithm("ApplyDiffCal")
        # set params
        applyDiffCal.execute()
    
    def sumNeighbours(self):
        self.reportAndIncrement("Summing Neighbours...")
        sumNeighbours = self.createChildAlgorithm("SumNeighbours")
        # set params
        sumNeighbours.execute()

    def createGroupWorkspace(self):
        self.reportAndIncrement("Creating Group Workspace...")
        createGroupWorkspace = self.createChildAlgorithm("CreateGroupWorkspace")
        # set params
        createGroupWorkspace.execute()

    def convertUnits(self):
        self.reportAndIncrement("Converting Units...")
        convertUnits = self.createChildAlgorithm("ConvertUnits")
        # set params
        convertUnits.execute()
    
    def cleanup(self):
        self._prog_reporter.report(self._endrange, "Done")
        self._progressCounter = 0

    def PyExec(self):
        reductionIngredients = self.getProperty("ReductionIngredients").value
        # run the algo
        self.log().notice("Execution of ReductionAlgorithm START!")
        # read inputs to variable


        # 1 LoadEventNexus            TODO: perform this elsewhere and cache it
        # 2 NormalizeByCurrent
        self.normalizeByCurrent()

        # 3 ApplyDiffCal
        self.applyDiffCal()

        # 4 Not Lite? SumNeighbours
        self.sumNeighbours()

        # 5 LoadNexus(Raw Vanadium)   TODO: perform this elsewhere and cache it
        # 6 Apply Calibration Mask to Raw Vanadium and Data output from SumNeighbours

        # 7 Does it have a container? Apply Container Mask to Raw Vanadium and Data output from SumNeighbours
        # 8 CreateGroupWorkspace      TODO: Assess performance, use alternative Andrei came up with that is faster
        self.createGroupWorkspace()

        # 9 Does it have a container? Apply Container Attenuation Correction
        # 10 ConvertUnits from TOF to dSpacing
        self.convertUnits()

        # 11 For each Group
        #  > DiffractionFocusing
        #  > StripPeaks
        #  > SmoothData
        #  > Mantid:Divide to apply Vanadium Correction
        #  > RebinRagged
        # Finally return status successful with references to outputed results.
        self.cleanup()
        self.log().notice("Execution of ReductionAlgorithm COMPLETE!")

# Register algorithm with Mantid
AlgorithmFactory.subscribe(ReductionAlgorithm)