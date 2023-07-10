import json

from mantid.api import AlgorithmFactory, PythonAlgorithm
from mantid.kernel import Direction

from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper

name = "CalculateDiffractionOffsets"


class CalculateDiffractionOffsets(PythonAlgorithm):
    def PyInit(self):
        # declare properties
        self.declareProperty(
            "DiffractionCalibrationIngredients", defaultValue="", direction=Direction.Input
        )  # noqa: F821
        self.declareProperty("InputWorkspace", defaultValue="", direction=Direction.Input)
        self.declareProperty("OutputWorkspace", defaultValue="", direction=Direction.Output)
        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, name)

    def chopIngredients(self, ingredients):
        self.dBin = -abs(ingredients.dBin)  # ensure dBin is negative for log binning
        self.runNumber = ingredients.runConfig.runNumber
        self.instrumentState = ingredients.instrumentState

        # ipts = ingredients.runConfig.IPTS

        # with open(stateInitFilename, "r") as json_file:
        #     self.instrumentState = json.load(json_file)
        # from state parameters, read the min/max TOF measures
        # self.TOFMin = self.instrumentState['instrumentState']['particleBounds']['tof']['minimum']
        # self.TOFMax = self.instrumentState['instrumentState']['particleBounds']['tof']['maximum']
        self.TOFMin = self.instrumentState["particleBounds"]["tof"]["minimum"]
        self.TOFMax = self.instrumentState["particleBounds"]["tof"]["maximum"]
        return

        # self.inputWStof = self.getProperty("InputWorkspace").value
        # self.inputWSdsp = self.inputWStof + '_dsp'
        # self.mantidSnapper.Rebin(
        #     "Rebin the TOF data",
        #     InputWorkspace=self.inputWStof,
        #     Params=f'{TOFMin},{TOFBin},{TOFMax}',
        #     OutputWorkspace=self.inputWStof,
        # )
        # self.mantidSnapper.ConvertUnits(
        #     "Convert the rebinned data to d-spacing units",
        #     InputWorkspace=self.inputWStof,
        #     OutputWorkspace=self.inputWSdsp,
        # )

        # # focused grouping ws
        # focusWSname = f'_{self.runNumber}_FocGroup'

        # if ext=='nxs':
        #     self.mantidSnapper.LoadNexusProcessed(
        #         "Load nexus grouping file",
        #         Filename=groupingFile,
        #         OutputWorkspace=focusWSname,
        #     )
        # elif ext=='xml':
        #     self.mantidSnapper.LoadDetectorsGroupingFile(
        #         "Load XML grouping file",
        #         InputFile = groupingFile,
        #         OutputWorkspace=focusWSname,
        #     )
        # else:
        #     throw RuntimeError("invalid file extension for groupingFile")
        # self.focusWS = self.mantidSnapper.mtd[focusWSname]
        # self.GroupIDs = self.focusWS.getGroupIDs()

        # #instantiate grouping workspace
        # snp.instantiateGroupingWS(sPrm,focusWS,isLite)
        # #get grouping specific parameters
        # self.gPrm = snp.initGroupingParams(sPrm,focusWS,isLite)
        # self.overallDMin = max(self.gPrm["dMin"])
        # self.overallDmax = min(self.gPrm["dMax"])

        # inputWS = f'_DSP_{run}_raw'
        # rebinWS = f'_DSP_{run}_raw_reb'
        # self.mantidSnapper.Rebin(
        #     "Logarithmically bin the d-spacing workspace",
        #     InputWorkspace=inputWS,
        #     Params=f'{self.overallDMin},{self.dBin},{self.overallDMax}',
        #     OutputWorkspace=rebinWS,
        # )

    def applyCalibration(self, calibrationWS):
        # apply offset correction to input workspace
        self.mantidSnapper.ApplyDiffCal(
            "Apply the diffraction calibration to the input TOF workspace",
            InstrumentWorkspace=self.inputWStof,
            CalibrationWorkspace=calibrationWS,
        )
        self.mantidSnapper.ConvertUnits(
            "Convert from TOF to d-spacing",
            InputWorkspace=self.inputWStof,
            OutputWorkspace=self.intputWSdsp,
            Target="dSpacing",
        )
        self.mantidSnapper.Rebin(
            "Rebin the d-spacing workspace logarithmically",
            InputWorkspace=inputWSdsp,
            Params=f"{self.overallDMin},{self.dBin},{self.overallDMax}",
            OutputWorkspace=inputWSdsp,
        )

    def reexecute(self, inputWS):
        # inputWS is a worksheet in d-space with logarithmic binning
        # difcWS is a worksheet holding the calibration constants, DIFC
        # for each group separately, find needed offsets
        totalOffsetWS = f"offsets_{self.runNumber}"
        wsoff = f"_{self.runNumber}_tmp_subgroup_offset"
        wscc = f"_{self.runNumber}_tmp_subgroup_CC"
        for subGroup in self.GroupIDs:
            subGroupIDs = self.getSubgroupIDs(subGroup)
            maxDspaceShift = self.getMaxDspaceShift(self.calibPeakListSorted)
            refID = self.getRefID(subGroupIDs)

            self.mantidSnapper.CrossCorrelate(
                f"Cross-Correlating spectra for {wscc}",
                InputWorkspace=self.inputWSdsp,
                OutputWorkspace=wscc,
                ReferenceSpectra=refID,
                WorkspaceIndexList=subGroupIDs,
                XMin=self.Xmin,
                XMax=self.Xmax,
                MaxDSpaceShift=maxDspaceShift,
            )
            self.mantidSnapper.GetDetectorOffsets(
                f"Calculate offset workspace {wsoff}",
                InputWorkspace=wscc,
                OutputWorkspace=wsoff,
                Step=abs(self.dBin),
                XMin=-100,
                XMax=100,
                OffsetMode="Signed",
            )

            try:
                self.mantidSnapper.Plus(
                    f"Addign in offset workspace {wsoff}",
                    LHSWorkspace=totalOffsetWS,
                    RHSWorkspace=wsoff,
                    OutputWorkspace=totalOffsetWS,
                )
            except ValueError:
                self.mantidSnapper.RenameWorkspace(
                    f"Starting summation with offset workspace {wsoff}",
                    InputWorkspace=wsoff,
                    OutputWorkspace=totalOffsetWS,
                )

        # offsets should converge to 0 with reexecution of the process
        # use the median, to avoid issues with possible pathologic pixels
        data["medianOffset"] = np.median(self.mantidSnapper.mtd[totalOffsetWS].extractY().ravel())

        # get difcal corrected by offsets
        calibrationWS = f"_{self.runNumber}_CAL_CC_temp"
        self.mantidSnapper.ConvertDifCalLog(
            "Correct previous calibration constants by offsets",
            OffsetsWorkspace=totalOffsetWS,
            PreviousCalibration=self.difcWS,
            OutputWorkspace=calibrationWS,
            BinWidth=self.dBin,
        )

        # save the resulting DIFC for starting point in next iteration
        # in the standard mantid form, this might noe be necessary
        self.mantidSnapper.ConvertTableToMatrixWorkspace(
            "Save the DIFC for starting point in next iteration",
            InputWorkspace=calibrationWS,
            OutputWorkspace=self.difcWS,
            ColumnX="detid",
            ColumnY="difc",
        )

        # apply difcal to input workspace
        self.getProperty("OutputWorkspace").value
        self.applyCalibration(calibrationWS)

        # cleanup memory usage
        self.mantidSnapper.DeleteWorkspace(
            f"Deleting temporary cross-correlation workspace {wscc}",
            Worksheet=wscc,
        )
        self.mantidSnapper.DeleteWorkspace(
            f"Deleting temporary offset workspace {wsname}",
            Workspace=wsoff,
        )
        self.mantidSnapper.DeleteWorkspace(
            "Deleting used calibration workspace",
            Workspace=calibrationWS,
        )
        self.mantidSnapper.DeleteWorkspace(
            "Deleting used offset workspace",
            Workspace=totalOffsetWS,
        )

        # now execute the queue
        self.mantidSnapper.executeQueue()

        return data

    def PyExec(self):
        # run the algo
        self.log().notice("Execution of extraction of calibration constants START!")

        # get the ingredients
        ingredients = ExtractionIngredients(**json.loads(self.getProperty("ExtractionIngredients").value))
        self.chopIngredients(ingredients)

        self.mantidSnapper.CalculateDIFC(
            InputWorkspace=self.inputWStof,
            OutputWorkspace=self.difcWS,
        )

        return self.reexecute(self.rebinWS)


# Register algorithm with Mantid
AlgorithmFactory.subscribe(CalculateDiffractionOffsets)
