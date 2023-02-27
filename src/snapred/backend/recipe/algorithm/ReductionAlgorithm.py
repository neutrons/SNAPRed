from mantid.kernel import *
from mantid.api import *

import time
import json

from snapred.backend.recipe.algorithm.CustomGroupWorkspace import name as CustomGroupWorkspace
from snapred.backend.dao.ReductionIngredients import ReductionIngredients

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
    _algorithmQueue = []

    def PyInit(self):
        # declare properties
        self.declareProperty('ReductionIngredients', defaultValue='', direction=Direction.Input)
        self.declareProperty('OutputWorkspace', defaultValue='', direction=Direction.Output)

    def createChildAlgorithm(self, name, isChild=True):
        alg = AlgorithmManager.create(name)
        alg.setChild(isChild)
        return alg

    def executeAlgorithm(self, name, isChild=True, **kwargs):
        algorithm = self.createChildAlgorithm(name, isChild)
        for prop, val in kwargs.items():
            algorithm.setProperty(prop, val)
        algorithm.execute()

    def enqueueAlgorithm(self, name, message, isChild=True, **kwargs):
        self._algorithmQueue.append((name, message, isChild, kwargs))
        self._endrange += 1

    def reportAndIncrement(self, message):
        self._prog_reporter.reportIncrement(self._progressCounter, message)
        self._progressCounter += 1

    def executeReduction(self):
        self._prog_reporter = Progress(self, start=0.0, end=1.0, nreports=self._endrange)
        for algorithmTuple in self._algorithmQueue:
            self.reportAndIncrement(algorithmTuple[1])
            self.log().notice(algorithmTuple[1])
            # import pdb; pdb.set_trace()
            self.executeAlgorithm(name=algorithmTuple[0], isChild=algorithmTuple[2], **algorithmTuple[3])

    
    def loadEventNexus(self, Filename, OutputWorkspace):
        self.enqueueAlgorithm("LoadEventNexus", "Loading Event Nexus for {} ...".format(Filename), False, Filename=Filename, OutputWorkspace=OutputWorkspace)
        return OutputWorkspace

    def loadNexus(self, Filename, OutputWorkspace):
        self.enqueueAlgorithm("LoadNexus","Loading Nexus for {} ...".format(Filename), False, Filename=Filename, OutputWorkspace=OutputWorkspace)
        return OutputWorkspace

    def loadDiffCal(self, Filename, InputWorkspace, WorkspaceName):
        self.enqueueAlgorithm("LoadDiffCal","Loading DiffCal for {} ...".format(Filename), False, InstrumentFilename='/SNS/SNAP/shared/Calibration/PixelGroupingDefinitions/SNAPLite.xml', Filename='/SNS/SNAP/shared/Calibration/14100-10/SNAP048705_calib_geom_20220907.lite.h5', MakeGroupingWorkspace=False, MakeMaskWorkspace=True, WorkspaceName=WorkspaceName)
        return WorkspaceName

    def normaliseByCurrent(self, InputWorkspace, OutputWorkspace):
        self.enqueueAlgorithm("NormaliseByCurrent", "Normalizing By Current...", InputWorkspace=InputWorkspace, OutputWorkspace=OutputWorkspace)
        return OutputWorkspace

    def applyDiffCal(self, InstrumentWorkspace, CalibrationWorkspace):
        self.enqueueAlgorithm("ApplyDiffCal", "Applying DiffCal...", False, InstrumentWorkspace=InstrumentWorkspace, CalibrationWorkspace=CalibrationWorkspace)
        return InstrumentWorkspace
    
    def sumNeighbours(self, InputWorkspace, SumX, SumY, OutputWorkspace):
        self.enqueueAlgorithm("SumNeighbours", "Summing Neighbours...", InputWorkspace=InputWorkspace, SumX=SumX, SumY=SumY, OutputWorkspace=OutputWorkspace)
        return OutputWorkspace
    
    def applyCalibrationPixelMask(self, Workspace, MaskedWorkspace):
        # always a pixel mask
        # loadmask
        # LoadMask(instrumentName=snap, MaskFile=".xml", OutputWorkspace="mask")
        # MaskDetectors(Workspace=Workspace, MaskedWorkspace=MaskedWorkspace)
        self.enqueueAlgorithm("MaskDetectors", "Applying Pixel Mask...", Workspace=Workspace, MaskedWorkspace=MaskedWorkspace)
        return Workspace

    # def applyContainerMask(self):
    #     # can be a pixel mask or bin mask(swiss cheese)  -- switch based on input param
    #     # loadmask
    #     # pixel
    #     LoadMask(instrumentName=snap, MaskFile=".xml", OutputWorkspace="containermask")
    #     MaskDetectors(Workspace=raw_data, MaskedWorkspace="mask")

    #     # bin
    #     # TODO: Homebrew Solution - ask Andrei/Malcolm  546 in the FocDacUtilities in the prototype
    #     # must be Unit aware, cannot cross units, -- please check and validate


    def createGroupWorkspace(self, StateConfig, InstrumentName, CalibrantWorkspace):

        self.enqueueAlgorithm(CustomGroupWorkspace, "Creating Group Workspace...", False, StateConfig=StateConfig.json(), InstrumentName=InstrumentName, CalibrantWorkspace=CalibrantWorkspace, OutputWorkspace='CommonRed')
        return 'CommonRed'

    def convertUnits(self, InputWorkspace, EMode, Target, OutputWorkspace, ConvertFromPointData):
        self.enqueueAlgorithm("ConvertUnits", "Converting to Units of {} ...".format(Target), False, InputWorkspace=InputWorkspace, EMode=EMode, Target=Target, OutputWorkspace=OutputWorkspace, ConvertFromPointData=ConvertFromPointData)
        return OutputWorkspace
    
    def diffractionFocusing(self, InputWorkspace, GroupingWorkspace, OutputWorkspace):
        self.enqueueAlgorithm("DiffractionFocussing", "Performing Diffraction Focusing ...", False, InputWorkspace=InputWorkspace, GroupingWorkspace=GroupingWorkspace, OutputWorkspace=OutputWorkspace)
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

    def rebinToWorkspace(self, WorkspaceToRebin, WorkspaceToMatch, OutputWorkspace, PreserveEvents):
        self.enqueueAlgorithm("RebinToWorkspace", "Rebinning to workspace...", WorkspaceToRebin=WorkspaceToRebin, WorkspaceToMatch=WorkspaceToMatch, OutputWorkspace=OutputWorkspace, PreserveEvents=PreserveEvents)
        return OutputWorkspace

    def rebinRagged(self, InputWorkspace, XMin, XMax, Delta, OutputWorkspace):
        self.enqueueAlgorithm("RebinRagged", "Rebinning ragged bins...", False, InputWorkspace=InputWorkspace, XMin=XMin, XMax=XMax, Delta=Delta, OutputWorkspace=OutputWorkspace)
        return OutputWorkspace

    def renameWorkspace(self, InputWorkspace, OutputWorkspace):
        self.enqueueAlgorithm("RenameWorkspace", "Renaming output workspace to something sensible...", False, InputWorkspace=InputWorkspace, OutputWorkspace=OutputWorkspace)
        return OutputWorkspace

    def cleanup(self):
        self._prog_reporter.report(self._endrange, "Done")
        self._progressCounter = 0
        self.algorithmQueue = []

    def PyExec(self):
        reductionIngredients = ReductionIngredients(**json.loads(self.getProperty("ReductionIngredients").value))
        # run the algo
        self.log().notice("Execution of ReductionAlgorithm START!")

        # TODO: Reorg how filepaths are stored
        ipts = reductionIngredients.runConfig.IPTS
        rawDataPath = ipts + 'shared/lite/SNAP_{}.lite.nxs.h5'.format(reductionIngredients.runConfig.runNumber)
        calibrationDirectory = reductionIngredients.reductionState.instrumentConfig.calibrationDirectory
        stateId = reductionIngredients.reductionState.stateConfig.stateId
        rawVanadiumCorrectionFileName = reductionIngredients.reductionState.stateConfig.rawVanadiumCorrectionFileName
        vanadiumFilePath = calibrationDirectory + stateId + '/powder/' + rawVanadiumCorrectionFileName
        diffCalPath = calibrationDirectory + stateId + '/powder/' + reductionIngredients.reductionState.stateConfig.diffractionCalibrant.filename
        
        raw_data = self.loadEventNexus(Filename=rawDataPath, OutputWorkspace="raw_data")    
        vanadium = self.loadNexus(Filename=vanadiumFilePath, OutputWorkspace="vanadium")

      # 2 NormalizeByCurrent -- just apply to data
        self.normaliseByCurrent(InputWorkspace=raw_data, OutputWorkspace=raw_data)


        # 4 Not Lite? SumNeighbours  -- just apply to data
        # self.sumNeighbours(InputWorkspace=raw_data, SumX=SuperPixEdge, SumY=SuperPixEdge, OutputWorkspace=raw_data)


        # 7 Does it have a container? Apply Container Mask to Raw Vanadium and Data output from SumNeighbours -- done to both data and vanadium
        # self.applyCotainerMask()
        # 8 CreateGroupWorkspace      TODO: Assess performance, use alternative Andrei came up with that is faster
        groupingworkspace = self.createGroupWorkspace(reductionIngredients.reductionState.stateConfig, reductionIngredients.reductionState.instrumentConfig.name, vanadium)

        # 3 ApplyDiffCal  -- just apply to data
        diffCalPrefix = self.loadDiffCal(Filename=diffCalPath, InputWorkspace='idf', WorkspaceName="diffcal")
        
        # 6 Apply Calibration Mask to Raw Vanadium and Data output from SumNeighbours -- done to both data, can be applied to vanadium per state
        self.applyCalibrationPixelMask(Workspace=raw_data, MaskedWorkspace=diffCalPrefix+"_mask")
        self.applyCalibrationPixelMask(Workspace=vanadium, MaskedWorkspace=diffCalPrefix+"_mask")
        
        self.applyDiffCal(InstrumentWorkspace=raw_data, CalibrationWorkspace=diffCalPrefix+"_cal")

        # 9 Does it have a container? Apply Container Attenuation Correction
        data = self.convertUnits(InputWorkspace=raw_data, EMode="Elastic", Target="dSpacing", OutputWorkspace="data", ConvertFromPointData=True)
        vanadium = self.convertUnits(InputWorkspace=vanadium, EMode="Elastic", Target="dSpacing", OutputWorkspace="vanadium_dspacing", ConvertFromPointData=True)

        # 11 For each Group (no for each loop, the algos apply things based on groups of group workspace)
        data = self.diffractionFocusing(InputWorkspace=data, GroupingWorkspace=groupingworkspace, OutputWorkspace='focused_data')
        vanadium = self.diffractionFocusing(InputWorkspace=vanadium, GroupingWorkspace=groupingworkspace, OutputWorkspace="diffraction_focused_vanadium")
        
        # sum chunks if files are large
        # TODO: Implement New Strip Peaks that allows for multiple FWHM, one per group, for now just grab the first one to get it to run
        peakPositions = ','.join(str(s) for s in reductionIngredients.reductionState.stateConfig.normalizationCalibrant.peaks)
        
        vanadium = self.stripPeaks(InputWorkspace=vanadium, FWHM=reductionIngredients.reductionState.stateConfig.focusGroups[0].FWHM[0], PeakPositions=peakPositions, OutputWorkspace='peaks_stripped_vanadium')
        vanadium = self.smoothData(InputWorkspace=vanadium, NPoints=reductionIngredients.reductionState.stateConfig.normalizationCalibrant.smoothPoints, OutputWorkspace='smoothed_data_vanadium')
        
        data = self.rebinToWorkspace(WorkspaceToRebin=data, WorkspaceToMatch=vanadium, OutputWorkspace="rebinned_data", PreserveEvents=True)
        data = self.divide(LHSWorkspace=data, RHSWorkspace=vanadium, OutputWorkspace='data_minus_vanadium')
        
        # TODO: Refactor so excute only needs to be called once
        self.executeReduction()
        self._algorithmQueue = []

        focusGroups = reductionIngredients.reductionState.stateConfig.focusGroups
        groupedData = data
        for workspaceIndex in range(len(focusGroups)): 
            data = self.rebinRagged(InputWorkspace=mtd[groupedData].getItem(workspaceIndex),
            XMin=focusGroups[workspaceIndex].dMin,
            XMax=focusGroups[workspaceIndex].dMax,
            Delta=focusGroups[workspaceIndex].dBin,
            OutputWorkspace='data_rebinned_ragged_'+str(focusGroups[workspaceIndex].name))

        # self.renameWorkspace(InputWorkspace=data, OutputWorkspace="SomethingSensible")

        self.executeReduction()

        self.cleanup()
        self.log().notice("Execution of ReductionAlgorithm COMPLETE!")
        return data

        # set outputworkspace to data

# Register algorithm with Mantid
AlgorithmFactory.subscribe(ReductionAlgorithm)