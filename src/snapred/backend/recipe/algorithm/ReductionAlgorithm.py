import json

from mantid.api import *
from mantid.kernel import *

from snapred.backend.dao.ReductionIngredients import ReductionIngredients
from snapred.backend.error.AlgorithmException import AlgorithmException
from snapred.backend.recipe.algorithm.CustomGroupWorkspace import name as CustomGroupWorkspace

name = "ReductionAlgorithm"


#######################################################
# ATTENTION: Could be replaced by alignAndFocusPowder #
# please confirm that attenutation correction before  #
# and after is equivalent                             #
#######################################################
class ReductionAlgorithm(PythonAlgorithm):
    _endrange = 0
    _progressCounter = 0
    _prog_reporter = None
    _algorithmQueue = []
    _exportScript = ""
    _export = False

    def PyInit(self):
        # declare properties
        self.declareProperty("ReductionIngredients", defaultValue="", direction=Direction.Input)
        self.declareProperty("OutputWorkspace", defaultValue="", direction=Direction.Output)
        self.setRethrows(True)

    def createAlgorithm(self, name, isChild=True):
        alg = AlgorithmManager.create(name)
        alg.setChild(isChild)
        alg.setAlwaysStoreInADS(True)
        alg.setRethrows(True)
        return alg

    def executeAlgorithm(self, name, isChild=True, **kwargs):
        algorithm = self.createAlgorithm(name, isChild)
        try:
            for prop, val in kwargs.items():
                algorithm.setProperty(prop, val)
            if not algorithm.execute():
                raise Exception("")
        except Exception as e:
            raise AlgorithmException(name, str(e))

    def enqueueAlgorithm(self, name, message, isChild=True, **kwargs):
        self._algorithmQueue.append((name, message, isChild, kwargs))
        self._endrange += 1

    def reportAndIncrement(self, message):
        self._prog_reporter.reportIncrement(self._progressCounter, message)
        self._progressCounter += 1

    def executeQueue(self):
        self._prog_reporter = Progress(self, start=0.0, end=1.0, nreports=self._endrange)
        for algorithmTuple in self._algorithmQueue:
            if self._export:
                self._exportScript += "{}(".format(algorithmTuple[0])
                for prop, val in algorithmTuple[3].items():
                    self._exportScript += "{}={}, ".format(
                        prop, val if not isinstance(val, str) else "'{}'".format(val)
                    )
                self._exportScript = self._exportScript[:-2]
                self._exportScript += ")\n"

            self.reportAndIncrement(algorithmTuple[1])
            self.log().notice(algorithmTuple[1])
            # import pdb; pdb.set_trace()
            self.executeAlgorithm(name=algorithmTuple[0], isChild=algorithmTuple[2], **algorithmTuple[3])

    def loadEventNexus(self, Filename, OutputWorkspace):
        self.enqueueAlgorithm(
            "LoadEventNexus",
            "Loading Event Nexus for {} ...".format(Filename),
            Filename=Filename,
            OutputWorkspace=OutputWorkspace,
        )
        return OutputWorkspace

    def loadNexus(self, Filename, OutputWorkspace):
        self.enqueueAlgorithm(
            "LoadNexus", "Loading Nexus for {} ...".format(Filename), Filename=Filename, OutputWorkspace=OutputWorkspace
        )
        return OutputWorkspace

    def loadDiffCal(self, Filename, WorkspaceName):
        self.enqueueAlgorithm(
            "LoadDiffCal",
            "Loading DiffCal for {} ...".format(Filename),
            InstrumentFilename="/SNS/SNAP/shared/Calibration/Powder/SNAPLite.xml",
            Filename=Filename,
            MakeGroupingWorkspace=False,
            MakeMaskWorkspace=True,
            WorkspaceName=WorkspaceName,
        )
        return WorkspaceName

    def normaliseByCurrent(self, InputWorkspace, OutputWorkspace):
        self.enqueueAlgorithm(
            "NormaliseByCurrent",
            "Normalizing By Current...",
            InputWorkspace=InputWorkspace,
            OutputWorkspace=OutputWorkspace,
        )
        return OutputWorkspace

    def applyDiffCal(self, InstrumentWorkspace, CalibrationWorkspace):
        self.enqueueAlgorithm(
            "ApplyDiffCal",
            "Applying DiffCal...",
            InstrumentWorkspace=InstrumentWorkspace,
            CalibrationWorkspace=CalibrationWorkspace,
        )
        return InstrumentWorkspace

    def sumNeighbours(self, InputWorkspace, SumX, SumY, OutputWorkspace):
        self.enqueueAlgorithm(
            "SumNeighbours",
            "Summing Neighbours...",
            InputWorkspace=InputWorkspace,
            SumX=SumX,
            SumY=SumY,
            OutputWorkspace=OutputWorkspace,
        )
        return OutputWorkspace

    def applyCalibrationPixelMask(self, Workspace, MaskedWorkspace):
        # always a pixel mask
        # loadmask
        # LoadMask(instrumentName=snap, MaskFile=".xml", OutputWorkspace="mask")
        # MaskDetectors(Workspace=Workspace, MaskedWorkspace=MaskedWorkspace)
        self.enqueueAlgorithm(
            "MaskDetectors", "Applying Pixel Mask...", Workspace=Workspace, MaskedWorkspace=MaskedWorkspace
        )
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

    def createGroupWorkspace(self, StateConfig, InstrumentName):
        self.enqueueAlgorithm(
            CustomGroupWorkspace,
            "Creating Group Workspace...",
            StateConfig=StateConfig.json(),
            InstrumentName=InstrumentName,
            OutputWorkspace="CommonRed",
        )
        return "CommonRed"

    def convertUnits(self, InputWorkspace, EMode, Target, OutputWorkspace, ConvertFromPointData):
        self.enqueueAlgorithm(
            "ConvertUnits",
            "Converting to Units of {} ...".format(Target),
            InputWorkspace=InputWorkspace,
            EMode=EMode,
            Target=Target,
            OutputWorkspace=OutputWorkspace,
            ConvertFromPointData=ConvertFromPointData,
        )
        self.deleteWorkspace(Workspace=InputWorkspace)
        return OutputWorkspace

    def diffractionFocusing(self, InputWorkspace, GroupingWorkspace, OutputWorkspace, PreserveEvents=False):
        self.enqueueAlgorithm(
            "DiffractionFocussing",
            "Performing Diffraction Focusing ...",
            InputWorkspace=InputWorkspace,
            GroupingWorkspace=GroupingWorkspace,
            OutputWorkspace=OutputWorkspace,
            PreserveEvents=PreserveEvents,
        )
        self.deleteWorkspace(Workspace=InputWorkspace)
        return OutputWorkspace

    def compressEvents(self, InputWorkspace, OutputWorkspace):
        self.enqueueAlgorithm(
            "CompressEvents", "Compressing events ...", InputWorkspace=InputWorkspace, OutputWorkspace=OutputWorkspace
        )
        self.deleteWorkspace(Workspace=InputWorkspace)
        return OutputWorkspace

    def stripPeaks(self, InputWorkspace, FWHM, PeakPositions, OutputWorkspace):
        self.enqueueAlgorithm(
            "StripPeaks",
            "Stripping peaks ...",
            InputWorkspace=InputWorkspace,
            FWHM=FWHM,
            PeakPositions=PeakPositions,
            OutputWorkspace=OutputWorkspace,
        )
        self.deleteWorkspace(Workspace=InputWorkspace)
        return OutputWorkspace

    def smoothData(self, InputWorkspace, NPoints, OutputWorkspace):
        self.enqueueAlgorithm(
            "SmoothData",
            "Smoothing Data ...",
            InputWorkspace=InputWorkspace,
            NPoints=NPoints,
            OutputWorkspace=OutputWorkspace,
        )
        self.deleteWorkspace(Workspace=InputWorkspace)
        return OutputWorkspace

    def divide(self, LHSWorkspace, RHSWorkspace, OutputWorkspace):
        self.enqueueAlgorithm(
            "Divide",
            "Dividing out vanadium from data ...",
            LHSWorkspace=LHSWorkspace,
            RHSWorkspace=RHSWorkspace,
            OutputWorkspace=OutputWorkspace,
        )
        self.deleteWorkspace(Workspace=LHSWorkspace)
        self.deleteWorkspace(Workspace=RHSWorkspace)
        return OutputWorkspace

    def rebinToWorkspace(self, WorkspaceToRebin, WorkspaceToMatch, OutputWorkspace, PreserveEvents):
        self.enqueueAlgorithm(
            "RebinToWorkspace",
            "Rebinning to workspace...",
            WorkspaceToRebin=WorkspaceToRebin,
            WorkspaceToMatch=WorkspaceToMatch,
            OutputWorkspace=OutputWorkspace,
            PreserveEvents=PreserveEvents,
        )
        self.deleteWorkspace(Workspace=WorkspaceToRebin)
        return OutputWorkspace

    def rebinRagged(self, InputWorkspace, XMin, XMax, Delta, OutputWorkspace):
        self.enqueueAlgorithm(
            "RebinRagged",
            "Rebinning ragged bins...",
            InputWorkspace=InputWorkspace,
            XMin=XMin,
            XMax=XMax,
            Delta=Delta,
            OutputWorkspace=OutputWorkspace,
        )
        return OutputWorkspace

    def renameWorkspace(self, InputWorkspace, OutputWorkspace):
        self.enqueueAlgorithm(
            "RenameWorkspace",
            "Renaming output workspace to something sensible...",
            InputWorkspace=InputWorkspace,
            OutputWorkspace=OutputWorkspace,
        )
        return OutputWorkspace

    def deleteWorkspace(self, Workspace):
        self.enqueueAlgorithm("DeleteWorkspace", "Freeing workspace...", Workspace=Workspace)

    def cleanup(self):
        self._prog_reporter.report(self._endrange, "Done")
        self._progressCounter = 0
        self.algorithmQueue = []

    def PyExec(self):
        reductionIngredients = ReductionIngredients(**json.loads(self.getProperty("ReductionIngredients").value))
        focusGroups = reductionIngredients.reductionState.stateConfig.focusGroups
        # run the algo
        self.log().notice("Execution of ReductionAlgorithm START!")

        # TODO: Reorg how filepaths are stored
        ipts = reductionIngredients.runConfig.IPTS
        rawDataPath = ipts + "shared/lite/SNAP_{}.lite.nxs.h5".format(reductionIngredients.runConfig.runNumber)
        vanadiumFilePath = reductionIngredients.reductionState.stateConfig.vanadiumFilePath
        diffCalPath = reductionIngredients.reductionState.stateConfig.diffractionCalibrant.diffCalPath

        raw_data = self.loadEventNexus(Filename=rawDataPath, OutputWorkspace="raw_data")
        vanadium = self.loadNexus(Filename=vanadiumFilePath, OutputWorkspace="vanadium")

        # 4 Not Lite? SumNeighbours  -- just apply to data
        # self.sumNeighbours(InputWorkspace=raw_data, SumX=SuperPixEdge, SumY=SuperPixEdge, OutputWorkspace=raw_data)

        # 7 Does it have a container? Apply Container Mask to Raw Vanadium and Data output from SumNeighbours -- done to both data and vanadium
        # self.applyCotainerMask()
        # 8 CreateGroupWorkspace      TODO: Assess performance, use alternative Andrei came up with that is faster
        groupingworkspace = self.createGroupWorkspace(
            reductionIngredients.reductionState.stateConfig, reductionIngredients.reductionState.instrumentConfig.name
        )

        # 3 ApplyDiffCal  -- just apply to data
        diffCalPrefix = self.loadDiffCal(Filename=diffCalPath, WorkspaceName="diffcal")

        # 6 Apply Calibration Mask to Raw Vanadium and Data output from SumNeighbours -- done to both data, can be applied to vanadium per state
        self.applyCalibrationPixelMask(Workspace=raw_data, MaskedWorkspace=diffCalPrefix + "_mask")
        self.applyCalibrationPixelMask(Workspace=vanadium, MaskedWorkspace=diffCalPrefix + "_mask")

        self.applyDiffCal(InstrumentWorkspace=raw_data, CalibrationWorkspace=diffCalPrefix + "_cal")

        self.deleteWorkspace(Workspace=diffCalPrefix + "_mask")
        self.deleteWorkspace(Workspace=diffCalPrefix + "_cal")
        self.deleteWorkspace(Workspace="idf")

        # 9 Does it have a container? Apply Container Attenuation Correction
        data = self.convertUnits(
            InputWorkspace=raw_data,
            EMode="Elastic",
            Target="dSpacing",
            OutputWorkspace="data",
            ConvertFromPointData=True,
        )
        vanadium = self.convertUnits(
            InputWorkspace=vanadium,
            EMode="Elastic",
            Target="dSpacing",
            OutputWorkspace="vanadium_dspacing",
            ConvertFromPointData=True,
        )

        # TODO: May impact performance of lite mode data
        # TODO: Params is supposed to be smallest dmin, smalled dbin, largest dmax
        # self.enqueueAlgorithm('Rebin', "Rebinning", isChild=False,  InputWorkspace=data, Params='0.338, -0.00086, 5.0', PreserveEvents=False, OutputWorkspace="rebinned_data_before_focus")
        # data = "rebinned_data_before_focus"
        # vanadium = self.enqueueAlgorithm('Rebin', "Rebinning", isChild=False, InputWorkspace=vanadium, Params='0.338, -0.00086, 5.0', PreserveEvents=False, OutputWorkspace="rebinned_vanadium_before_focus")
        # vanadium = "rebinned_vanadium_before_focus"
        # 11 For each Group (no for each loop, the algos apply things based on groups of group workspace)
        data = self.diffractionFocusing(
            InputWorkspace=data, GroupingWorkspace=groupingworkspace, OutputWorkspace="focused_data"
        )
        vanadium = self.diffractionFocusing(
            InputWorkspace=vanadium, GroupingWorkspace=groupingworkspace, OutputWorkspace="diffraction_focused_vanadium"
        )

        # 2 NormalizeByCurrent -- just apply to data
        self.normaliseByCurrent(InputWorkspace=data, OutputWorkspace=data)

        # self.deleteWorkspace(Workspace=rebinned_data_before_focus)
        self.deleteWorkspace(Workspace="CommonRed")

        # compress data
        # data = self.compressEvents(InputWorkspace=data, OutputWorkspace='event_compressed_data')

        # sum chunks if files are large
        # TODO: Implement New Strip Peaks that allows for multiple FWHM, one per group, for now just grab the first one to get it to run
        peakPositions = ",".join(
            str(s) for s in reductionIngredients.reductionState.stateConfig.normalizationCalibrant.peaks
        )

        vanadium = self.stripPeaks(
            InputWorkspace=vanadium,
            FWHM=reductionIngredients.reductionState.stateConfig.focusGroups[0].FWHM[0],
            PeakPositions=peakPositions,
            OutputWorkspace="peaks_stripped_vanadium",
        )
        vanadium = self.smoothData(
            InputWorkspace=vanadium,
            NPoints=reductionIngredients.reductionState.stateConfig.normalizationCalibrant.smoothPoints,
            OutputWorkspace="smoothed_data_vanadium",
        )

        data = self.rebinToWorkspace(
            WorkspaceToRebin=data, WorkspaceToMatch=vanadium, OutputWorkspace="rebinned_data", PreserveEvents=False
        )
        data = self.divide(LHSWorkspace=data, RHSWorkspace=vanadium, OutputWorkspace="data_minus_vanadium")

        # TODO: Refactor so excute only needs to be called once
        self.executeQueue()
        self._algorithmQueue = []

        groupedData = data
        for workspaceIndex in range(len(focusGroups)):
            data = self.rebinRagged(
                InputWorkspace=mtd[groupedData].getItem(workspaceIndex),
                XMin=focusGroups[workspaceIndex].dMin,
                XMax=focusGroups[workspaceIndex].dMax,
                Delta=focusGroups[workspaceIndex].dBin,
                OutputWorkspace="data_rebinned_ragged_" + str(focusGroups[workspaceIndex].name),
            )
        self.deleteWorkspace(Workspace="data_minus_vanadium")
        # self.renameWorkspace(InputWorkspace=data, OutputWorkspace="SomethingSensible")

        self.executeQueue()

        if self._export:
            with open("/SNS/users/wqp/git/snapred/snap_reduction.py", "w") as file:
                file.write(self._exportScript)

        self.cleanup()
        self.log().notice("Execution of ReductionAlgorithm COMPLETE!")
        return data

        # set outputworkspace to data


# Register algorithm with Mantid
AlgorithmFactory.subscribe(ReductionAlgorithm)
