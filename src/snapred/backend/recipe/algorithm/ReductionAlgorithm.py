from mantid.kernel import *
from mantid.api import *
import time

from mantid.api import AlgorithmManager

from snapred.backend.recipe.algorithm.CustomGroupWorkspace import name as CustomGroupWorkspace

name = "ReductionAlgorithm"

#######################################################
# ATTENTION: Could be replaced by alignAndFocusPowder #
# please confirm that attenutation correction before  #
# and after is equivalent                             #
#######################################################
class ReductionAlgorithm(PythonAlgorithm):

    _endrange=0
    _progressCounter = 0
    _prog_reporter = None
    algorithmQueue = []

    def PyInit(self):
        # declare properties
        self.declareProperty('ReductionIngredients', None)

    def createChildAlgorithm(self, name):
        alg = AlgorithmManager.create(name)
        alg.setChild(True)
        return alg

    def executeAlgorithm(self, name, **kwargs)
        algorithm = self.createChildAlgorithm(name)
        for prop, val in kwargs:
            algorithm.setProperty(prop, val)
        algorithm.execute()

    def enqueueAlgorithm(self, name, message, **kwargs):
        self.algorithmQueue.append([name, message, kwargs])
        self._endrange += 1

    def reportAndIncrement(self, _progressCounter, message):
        self._prog_reporter.reportIncrement(_progressCounter, message)
        self._progressCounter += 1

    def executeReduction(self):
        self._prog_reporter = Progress(self, start=0.0, end=1.0, nreports=_endrange)
        for algorithmTuple in self.algorithmQueue:
            self.reportAndIncrement(algorithmTuple[1])
            self.executeAlgorithm(algorithmTuple[0], **(algorithmTuple[2]))

    
    def loadEventNexus(self, Filename, OutputWorkspace):
        self.enqueueAlgorithm("LoadEventNexus", "Loading Event Nexus for {} ...".format(Filename), Filename=Filename, OutputWorkspace=OutputWorkspace)
        return OutputWorkspace

    def loadNexus(self, Filename, OutputWorkspace):
        self.enqueueAlgorithm("LoadNexus","Loading Event Nexus for {} ...".format(Filename), Filename=Filename, OutputWorkspace=OutputWorkspace)
        return OutputWorkspace

    def normalizeByCurrent(self, InputWorkspace, OutputWorkspace):
        self.enqueueAlgorithm("NormalizeByCurrent", "Normalizing By Current...", InputWorkspace=InputWorkspace, OutputWorkspace=OutputWorkspace)
        return OutputWorkspace

    def applyDiffCal(self, InputWorkspace, CalibrationWorkspace):
        self.enqueueAlgorithm("ApplyDiffCal", "Applying DiffCal...", InputWorkspace=InputWorkspace, CalibrationWorkspace=CalibrationWorkspace)
        return InputWorkspace
    
    def sumNeighbours(self, InputWorkspace, SumX, SumY, OutputWorkspace):
        self.enqueueAlgorithm("SumNeighbours", "Summing Neighbours...", InputWorkspace=InputWorkspace, SumX=SumX, SumY=SumY, OutputWorkspace=OutputWorkspace)
        return OutputWorkspace
    
    # def applyCalibrationPixelMask(self):
    #     # always a pixel mask
    #     # loadmask
    #     LoadMask(instrumentName=snap, MaskFile=".xml", OutputWorkspace="mask")
    #     MaskDetectors(Workspace=raw_data, MaskedWorkspace="mask")
    #     return OutputWorkspace

    # def applyContainerMask(self):
    #     # can be a pixel mask or bin mask(swiss cheese)  -- switch based on input param
    #     # loadmask
    #     # pixel
    #     LoadMask(instrumentName=snap, MaskFile=".xml", OutputWorkspace="containermask")
    #     MaskDetectors(Workspace=raw_data, MaskedWorkspace="mask")

    #     # bin
    #     # TODO: Homebrew Solution - ask Andrei/Malcolm  546 in the FocDacUtilities in the prototype
    #     # must be Unit aware, cannot cross units, -- please check and validate


    def createGroupWorkspace(self, FocusGroups, InstrumentName):
        self.enqueueAlgorithm(CustomGroupWorkspace, "Creating Group Workspace...", FocusGroups=FocusGroups, InstrumentName=InstrumentName, OutputWorkspace='CommonRed')
        return 'CommonRed'

    def convertUnits(self, InputWorkspace, EMde, Target, OutputWorkspace, ConvertFromPointData):
        self.enqueueAlgorithm("ConvertUnits", "Converting to Units of {} ...".format(Target), InputWorkspace=InputWorkspace, EMde=EMde, Target=Target, OutputWorkspace=OutputWorkspace, ConvertFromPointData=ConvertFromPointData)
        return OutputWorkspace
    
    def diffractionFocusing(self, InputWorkspace, GroupingWorkspace, OutputWorkspace):
        self.enqueueAlgorithm("DiffractionFocussing", "Performing Diffraction Focusing ...", InputWorkspace=InputWorkspace, GroupingWorkspace=GroupingWorkspace, OutputWorkspace=OutputWorkspace)
        return OutputWorkspace
    
    def stripPeaks(self, InputWorkspace, FWHM, PeakPositions, OutputWorkspace):
        self.enqueueAlgorithm("StripPeaks", "Stripping peaks ...", InputWorkspace=InputWorkspace, FWHM=FWHM, PeakPositions=PeakPositions, OutputWorkspace=OutputWorkspace)
        return OutputWorkspace

    def smoothData(self, InputWorkspace, NPoints, OutputWorkspace):
        self.enqueueAlgorithm("SmoothData", "Smoothing Data ...", InputWorkspace=InputWorkspace, NPoints=NPoints, OutputWorkspace=OutputWorkspace)
        return OutputWorkspace

    def divide(self, LHSWorkspace, RHSWorkspace, OutputWorkspace):
        self.enqueueAlgorithm("Divide", "Dividing out vanadium from data ...", LHSWorkspace=LHSWorkspace, RHSWorkspace=RHSWorkspace, OutputWorkspace=OutputWorkspace)
        return OutputWorkspace

    def rebinRagged(InputWorkspace, XMin, XMax, Delta, OutputWorkspace):
        self.enqueueAlgorithm("RebingRagged", "Rebinning ragged bins...", XMin=XMin, XMax=XMax, Delta=Delta, OutputWorkspace=OutputWorkspace)
        return OutputWorkspace

    def renameWorkspace(self, InputWorkspace, OutputWorkspace):
        self.enqueueAlgorithm("RenameWorkspace", "Renaming output workspace to something sensible...", InputWorkspace=InputWorkspace, OutputWorkspace=OutputWorkspace)
        return OutputWorkspace

    def cleanup(self):
        self._prog_reporter.report(self._endrange, "Done")
        self._progressCounter = 0
        self.algorithmQueue = []

    def PyExec(self):
        self._prog_reporter = Progress(self, start=0.0, end=1.0, nreports=_endrange)
        reductionIngredients = self.getProperty("ReductionIngredients").value
        # run the algo
        self.log().notice("Execution of ReductionAlgorithm START!")
        # read inputs to variable


        # 1 LoadEventNexus (Raw Data)
        raw_data = self.loadEventNexus(Filename=, OutputWorkspace="raw_data")
        # 5 LoadNexus(Vanadium)     
        vanadium = self.loadNexus(Filename=, OutputWorkspace="vanadium")
        # 2 NormalizeByCurrent -- just apply to data
        self.normalizeByCurrent(InputWorkspace=raw_data, OutputWorkspace=raw_data)

        # 3 ApplyDiffCal  -- just apply to data
        self.applyDiffCal(InputWorkspace=raw_data, CalibrationWorkspace=reductionIngredients.reductionState.stateConfig.diffractionCalibrant.name)

        # 4 Not Lite? SumNeighbours  -- just apply to data
        # self.sumNeighbours(InputWorkspace=raw_data, SumX=SuperPixEdge, SumY=SuperPixEdge, OutputWorkspace=raw_data)


        # 6 Apply Calibration Mask to Raw Vanadium and Data output from SumNeighbours -- done to both data, can be applied to vanadium per state
        # self.applyCalibrationPixelMask()

        # 7 Does it have a container? Apply Container Mask to Raw Vanadium and Data output from SumNeighbours -- done to both data and vanadium
        # self.applyCotainerMask()
        # 8 CreateGroupWorkspace      TODO: Assess performance, use alternative Andrei came up with that is faster
        groupingworkspace = self.createGroupWorkspace(reductionIngredients.reductionState.stateConfig.focusGroups, reductionIngredients.reductionState.instrumentConfig.name)

        # 9 Does it have a container? Apply Container Attenuation Correction
        # 10 ConvertUnits from TOF to dSpacing  -- just apply to data
        data = self.convertUnits(InputWorkspace=raw_data, EMde="elastic", Target="dspacing", OutputWorkspace="data", ConvertFromPointData=True)

        # 11 For each Group (no for each loop, the algos apply things based on groups of group workspace)
        #> DiffractionFocusing  -- done to both data and vanadium
        self.diffractionFocusing(InputWorkspace=data, GroupingWorkspace=groupingworkspace, OutputWorkspace=data)
        diffraction_focused_vanadium = self.diffractionFocusing(InputWorkspace=vanadium, GroupingWorkspace=groupingworkspace, OutputWorkspace="diffraction_focused_vanadium")
        
        # sum chunks if files are large
        #> StripPeaks  -- applied just to vanadium
        # TODO: Implement New Strip Peaks that allows for multiple FWHM, one per group, for now just grab the first one to get it to run
        self.stripPeaks(InputWorkspace=diffraction_focused_vanadium, FWHM=reductionIngredients.reductionState.stateConfig.focusGroups[0].FWHM, PeakPositions=reductionIngredients.reductionState.stateConfig.normaliztionCalibrant.peaks, OutputWorkspace=diffraction_focused_vanadium)
        #> SmoothData  -- applied just to vanadium
        self.smoothData(InputWorkspace=diffraction_focused_vanadium, NPoints=reductionIngredients.reductionState.stateConfig.normaliztionCalibrant.smoothPoints OutputWorkspace=diffraction_focused_vanadium)
        #> Mantid:Divide to apply Vanadium Correction
        self.divide(LHSWorkspace=data, RHSWorkspace=diffraction_focused_vanadium, OutputWorkspace=data)
        #> RebinRagged
        self.rebinRagged(InputWorkspace=data, XMin=reductionIngredients.reductionState.stateConfig.dMin, XMax=reductionIngredients.reductionState.stateConfig.dMax, Delta=reductionIngredients.reductionState.stateConfig.dBin, OutputWorkspace=data)
        # Finally return status successful with references to outputed results.
        self.renameWorkspace(InputWorkspace=data, OutputWorkspace="SomethingSenisble")


        self.executeReduction()


        self.cleanup()
        self.log().notice("Execution of ReductionAlgorithm COMPLETE!")
        return data

        # set outputworkspace to data

# Register algorithm with Mantid
AlgorithmFactory.subscribe(ReductionAlgorithm)