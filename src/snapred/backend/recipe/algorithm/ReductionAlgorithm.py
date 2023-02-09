from mantid.kernel import *
from mantid.api import *
import time

from mantid.api import AlgorithmManager

name = "ReductionAlgorithm"

#######################################################
# ATTENTION: Could be replaced by alignAndFocusPowder #
# please confirm that attenutation correction before  #
# and after is equivalent                             #
#######################################################
class ReductionAlgorithm(PythonAlgorithm):

    _endrange=11
    _progressCounter = 0
    _prog_reporter = None

    def PyInit(self):
        # declare properties
        self.declareProperty('ReductionIngredients', None)

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
    
    def applyCalibrationPixelMask(self):
        # always a pixel mask
        # loadmask
        LoadMask(instrumentName=snap, MaskFile=".xml", OutputWorkspace="mask")
        MaskDetectors(Workspace=raw_data, MaskedWorkspace="mask")

    def applyCotainerMask(self):
        # can be a pixel mask or bin mask(swiss cheese)  -- switch based on input param
        # loadmask
        # pixel
        LoadMask(instrumentName=snap, MaskFile=".xml", OutputWorkspace="containermask")
        MaskDetectors(Workspace=raw_data, MaskedWorkspace="mask")

        # bin
        # TODO: Homebrew Solution - ask Andrei/Malcolm  546 in the FocDacUtilities in the prototype
        # must be Unit aware, cannot cross units, -- please check and validate


    def createGroupWorkspace(self, focusGroups, instrumentName):
        self.reportAndIncrement("Creating Group Workspace...")
        # createGroupWorkspace = self.createChildAlgorithm("CreateGroupWorkspace")
        CreateGroupingWorkspace(InputWorkspace='TOF_rawVmB',GroupDetectorsBy='Column',OutputWorkspace='gpTemplate')

        for grpIndx,focusGroup in enumerate(focusGroups):
            CloneWorkspace(InputWorkspace='gpTemplate',
            OutputWorkspace=f'{instrumentName}{focusGroup.name}Gp')

            currentWorkspaceName = f'{instrumentName}{focusGroup.name}Gp'

            ws = mtd[currentWorkspaceName]
            nh = ws.getNumberHistograms()
            NSubGrp = len(focusGroup.definition)
            # print(f'creating grouping for {focusGroup} with {NSubGrp} subgroups')
            for pixel in range(nh):
                ws.setY(pixel,np.array([0.0])) #set to zero to ignore unless pixel is defined as part of group beklow.
                for subGrp in range(NSubGrp):
                    if pixel in focusGroup.definition[subGrp]:
                        ws.setY(pixel,np.array([subGrp+1]))

            gpString = gpString + ',' + currentWorkspaceName

            GroupWorkspaces(InputWorkspaces=gpString,
                OutputWorkspace='CommonRed'
                )

        print('State pixel groups initialised')
        DeleteWorkspace(Workspace='gpTemplate')


        
        
        
        # set params
        createGroupWorkspace.execute()

    def convertUnits(self):
        self.reportAndIncrement("Converting Units...")
        convertUnits = self.createChildAlgorithm("ConvertUnits")
        # set params
        convertUnits.execute()
    
    def diffractionFocusing():
        pass
    def stripPeaks():
        pass
    def smoothData():
        pass
    def divide():
        pass
    def rebinRagged():
        pass

    def cleanup(self):
        self._prog_reporter.report(self._endrange, "Done")
        self._progressCounter = 0

    def PyExec(self):
        self._prog_reporter = Progress(self, start=0.0, end=1.0, nreports=_endrange)
        reductionIngredients = self.getProperty("ReductionIngredients").value
        # run the algo
        self.log().notice("Execution of ReductionAlgorithm START!")
        # read inputs to variable


        # # 1 LoadEventNexus (Raw Data)
        # # 5 LoadNexus(Raw Vanadium)     
        
        # # 2 NormalizeByCurrent -- just apply to data
        # self.normalizeByCurrent(InputWorkspace=raw_data, OutputWorkspace=raw_data)

        # # 3 ApplyDiffCal  -- just apply to data
        # self.applyDiffCal(InputWorkspace=raw_data, CalibrationWorkspace=calibrationworkspace)

        # # 4 Not Lite? SumNeighbours  -- just apply to data
        # self.sumNeighbours(InputWorkspace=raw_data, SumX=SuperPixEdge, SumY=SuperPixEdge, OutputWorkspace=raw_data)


        # # 6 Apply Calibration Mask to Raw Vanadium and Data output from SumNeighbours -- done to both data, can be applied to vanadium per state
        # self.applyCalibrationPixelMask()

        # # 7 Does it have a container? Apply Container Mask to Raw Vanadium and Data output from SumNeighbours -- done to both data and vanadium
        # self.applyCotainerMask()
        # # 8 CreateGroupWorkspace      TODO: Assess performance, use alternative Andrei came up with that is faster
        # groupingworkspace = self.createGroupWorkspace(FocusGroup)

        # # 9 Does it have a container? Apply Container Attenuation Correction
        # # 10 ConvertUnits from TOF to dSpacing  -- just apply to data
        # self.convertUnits(inputworkspace=, emode="elastic", target="dspacing", outputworkspace=, convertfrompointdata=True)

        # # 11 For each Group
        # #> DiffractionFocusing  -- done to both data and vanadium
        # self.diffractionFocusing(inputworkspace=data, groupingworkspace=groupingworkspace outputworkspace=)
        # self.diffractionFocusing(inputworkspace=vanadium, groupingworkspace=groupingworkspace outputworkspace=)
        
        # # sum chunks if files are large
        # #> StripPeaks  -- applied just to vanadium
        # # TODO: Implement New Strip Peaks that allows for multiple FWHM, one per group
        # self.stripPeaks(inputworkspace=diffraction_focused_vanadium, FWHM=FocusGroup.FWHM, PeakPositions= NormaliztionCalibrant.peaks, outputworkspace=diffraction_focused_vanadium)
        # #> SmoothData  -- applied just to vanadium
        # self.smoothData(inputworkspace=diffraction_focused_vanadium, npoints=NormalizationCalibrant.smoothPoints outputworkspace=diffraction_focused_vanadium)
        # #> Mantid:Divide to apply Vanadium Correction
        # self.divide(lhsworkspace=data, rhsworkspace=diffraction_focused_vanadium, outputworkspace=data)
        # #> RebinRagged
        # self.rebinRagged(inputworkspace=data, Xmin=StateConfig.dMin, xMax=StateConfig.dMax, delta=StateConfig.dBin, outputworkspace=data)
        # # Finally return status successful with references to outputed results.

        # TODO: renameworkspace something useful
        self.cleanup()
        self.log().notice("Execution of ReductionAlgorithm COMPLETE!")
        return True

        # set outputworkspace to data

# Register algorithm with Mantid
AlgorithmFactory.subscribe(ReductionAlgorithm)