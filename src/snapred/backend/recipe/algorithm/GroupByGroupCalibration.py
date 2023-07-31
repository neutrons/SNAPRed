import json
from typing import Dict, List

from mantid.api import AlgorithmFactory, PythonAlgorithm
from mantid.kernel import Direction

from snapred.backend.dao.DiffractionCalibrationIngredients import DiffractionCalibrationIngredients
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper

name = "GroupByGroupCalibration"


class GroupByGroupCalibration(PythonAlgorithm):
    def PyInit(self):
        # declare properties
        self.declareProperty("Ingredients", defaultValue="", direction=Direction.Input)  # noqa: F821
        self.declareProperty("InputWorkspace", defaultValue="", direction=Direction.Input)
        self.declareProperty("PreviousCalibrationTable", defaultValue="", direction=Direction.Input)
        self.declareProperty("OutputWorkspace", defaultValue="", direction=Direction.Output)
        self.declareProperty("FinalCalibrationTable", defaultValue="", direction=Direction.Output)
        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, name)

    def chopIngredients(self, ingredients):
        """Receive the ingredients from the recipe, and exctract the needed pieces for this algorithm."""
        from datetime import date

        """Receive the ingredients from the recipe, and exctract the needed pieces for this algorithm."""
        self.runNumber: str = ingredients.runConfig.runNumber
        self.ipts: str = ingredients.runConfig.IPTS
        self.rawDataPath: str = self.ipts + "shared/lite/SNAP_{}.lite.nxs.h5".format(ingredients.runConfig.runNumber)

        # TODO setup for SNAPLite
        self.isLite = False
        self.stateFolder: str = ingredients.calPath

        # set output filename
        self.outputFilename: str
        if self.isLite:
            self.outputFilename = (
                f'{self.stateFolder}SNAP{self.runNumber}_calib_geom_{date.today().strftime("%Y%m%d")}.lite.h5'
            )
        else:
            self.outputFilename = (
                f'{self.stateFolder}SNAP{self.runNumber}_calib_geom_{date.today().strftime("%Y%m%d")}.h5'
            )

        # from the instrument state, read the overall min/max TOF
        self.TOFMin: float = ingredients.instrumentState.particleBounds.tof.minimum
        self.TOFMax: float = ingredients.instrumentState.particleBounds.tof.maximum
        self.TOFBin: float = -0.001  # must be negative for log binning, value chosen by @mguthriem

        # from grouping parameters, read the overall min/max d-spacings
        self.overallDMin: float = max(ingredients.focusGroup.dMin)
        self.overallDMax: float = min(ingredients.focusGroup.dMax)
        self.dBin: float = -min(
            [abs(x) for x in ingredients.focusGroup.dBin]
        )  # ensure dBin is negative for log binning
        self.maxDSpaceShifts: float = 2.5 * max(ingredients.focusGroup.FWHM)

        # path to grouping file, specifying group IDs of pixels
        self.groupingFile: str = ingredients.focusGroup.definition

        # for each group, need a list of peaks and boundaries of peaks
        # this is found from the groupedPeakList in the ingredients
        self.groupIDs: List[int] = []
        self.groupedPeaks: Dict[int, List[float]] = {}
        self.groupedPeakBoundaries: Dict[int, List[float]] = {}
        for peakList in ingredients.groupedPeakLists:
            self.groupIDs.append(peakList.groupID)
            allPeaks: List[float] = []
            allPeakBoundaries: List[float] = []
            for peak in peakList.peaks:
                allPeaks.append(peak.value)
                allPeakBoundaries.append(peak.minimum)
                allPeakBoundaries.append(peak.maximum)
            self.groupedPeaks[peakList.groupID] = allPeaks
            self.groupedPeakBoundaries[peakList.groupID] = allPeakBoundaries

        self.inputWStof: str = self.getProperty("InputWorkspace").value
        self.calibrationTable: str = self.getProperty("PreviousCalibrationTable").value
        self.diffractionfocusedWStof: str = f"_TOF_{self.runNumber}_diffoc"

    def retrieveFromPantry(self):
        """Load required data, if not already loaded, and process it"""

        if not self.mantidSnapper.mtd.doesExist(self.inputWStof):
            # TODO allow for loading alternate file types
            self.mantidSnapper.LoadEventNexus(
                "Loading Event Nexus for {} ...".format(self.rawDataPath),
                Filename=self.rawDataPath,
                OutputWorkspace=self.inputWStof,
                FilterByTofMin=self.TOFMin,
                FilterByTofMax=self.TOFMax,
                BlockList="Phase*,Speed*,BL*:Chop:*,chopper*TDC",
            )
        self.mantidSnapper.Rebin(
            "Rebin the workspace logarithmically",
            InputWorkspace=self.inputWStof,
            Params=f"{self.TOFMin},{-abs(self.TOFBin)},{self.TOFMax}",
            OutputWorkspace=self.inputWStof,
        )

        # also find d-spacing data and rebin logarithmically
        inputWSdsp: str = f"_DSP_{self.runNumber}"
        self.mantidSnapper.ConvertUnits(
            "Convert units to d-spacing",
            InputWorkspace=self.inputWStof,
            OutputWorkspace=inputWSdsp,
            Target="dSpacing",
        )
        self.mantidSnapper.Rebin(
            "Rebin the workspace logarithmically",
            InputWorkspace=inputWSdsp,
            Params=f"{self.overallDMin},{-abs(self.dBin)},{self.overallDMax}",
            OutputWorkspace=inputWSdsp,
        )

        # now diffraction focus the d-spacing data and conver to TOF
        focusWSname = f"_{self.runNumber}_focusGroup"
        self.mantidSnapper.LoadDetectorsGroupingFile(
            "Load XML grouping file",
            InputFile=self.groupingFile,
            OutputWorkspace=focusWSname,
        )

        diffractionfocusedWSdsp: str = f"_DSP_{self.runNumber}_diffoc"
        self.mantidSnapper.DiffractionFocussing(
            "Refocus with offset-corrections",
            InputWorkspace=inputWSdsp,
            GroupingWorkspace=focusWSname,
            OutputWorkspace=diffractionfocusedWSdsp,
        )
        self.mantidSnapper.ConvertUnits(
            "Convert units to TOF",
            InputWorkspace=diffractionfocusedWSdsp,
            OutputWorkspace=self.diffractionfocusedWStof,
            Target="TOF",
        )
        self.mantidSnapper.Rebin(
            "Rebin the workspace logarithmically",
            InputWorkspace=self.diffractionfocusedWStof,
            Params=f"{self.TOFMin},{-abs(self.TOFBin)},{self.TOFMax}",
            OutputWorkspace=self.diffractionfocusedWStof,
        )
        # clean up d-spacing workspaces
        self.mantidSnapper.DeleteWorkspace(
            "Clean up d-spacing data",
            Workspace=focusWSname,
        )
        self.mantidSnapper.DeleteWorkspace(
            "Clean up d-spacing data",
            Workspace=inputWSdsp,
        )
        self.mantidSnapper.Deleteworkspace(
            "Clean up d-spaced diffraction focused data",
            Workspace=diffractionfocusedWSdsp,
        )
        self.mantidSnapper.executeQueue()

    def storeInPantry(self) -> None:
        """
        Save the calculated diffraction calibration table to file.
        Will be saved inside the state folder, with name of form
            `/stateFolder/SNAP{run number}_calib_geom_{today's date}.h5`
        """
        self.mantidSnapper.SaveDiffCal(
            f"Saving the Diffraction Calibration table to {self.outputFilename}",
            CalibrationWorkspace=self.getProperty("FinalCalibrationTable").value,
            Filename=self.outputFilename,
        )
        self.mantidSnapper.executeQueue()

    def PyExec(self) -> None:
        """
        Execute the group-by-group calibration algorithm.
        First a table of previous pixel calibrations must be loaded.
        Then, group-by-group, each spectrum is calibrated by fitting peaks, and the
        resulting DIFCs are combined with the previous table.
        The final calibration table is saved to disk for future use.
        input:
            Ingredients: DiffractionCalibrationIngredients -- the DAO holding data needed to run the algorithm
            InputWorkspace: str -- the name of workspace holding the initial TOF data
            PreviousCalibrationTable: str -- the name of the table workspace with previous DIFC values
        output:
            OutputWorkspace: str -- the name of the diffraction-focussed d-spacing data after the final calibration
            FinalCalibrationTable: str -- the name of the final table of DIFC values
        """
        # run the algo
        self.log().notice("Execution of extraction of calibration constants START!")

        # get the ingredients
        ingredients = DiffractionCalibrationIngredients.parse_raw(self.getProperty("Ingredients").value)
        self.chopIngredients(ingredients)
        self.retrieveFromPantry()

        pdcalibratedWorkspace = "_tmp_PDCal_subgroup"

        diffocWS = self.mantidSnapper.mtd[self.diffractionfocusedWStof]
        nHist = diffocWS.getNumberHistograms()
        if nHist != len(self.groupIDs):
            raise RuntimeError("error, the number of spectra in focused workspace, and number of groups, do not match")

        for index in range(nHist):
            groupID = self.groupIDs[index]
            self.mantidSnapper.PDCalibration(
                f"Perform PDCalibration on subgroup {groupID}",
                InputWorkspace=self.diffractionfocusedWStof,
                TofBinning=f"{self.TOFMin},{self.TOFBin},{self.TOFMax}",
                PeakFunction="Gaussian",
                BackgroundType="Linear",
                PeakPositions=self.groupedPeaks[groupID],
                PeakWindow=self.groupedPeakBoundaries[groupID],
                CalibrationParameters="DIFC",
                HighBackground=True,  # vanadium must use high background to FitPeaks
                OutputCalibrationTable=pdcalibratedWorkspace,
                DiagnosticWorkspaces=f"_PDCal_diag_{groupID}",
                # limit to specific spectrum
                StartWorkspaceIndex=index,
                StopWorkspaceIndex=index,
            )
            self.mantidSnapper.DeleteWorkspace(
                "Cleanup needles diagnostic workspace",
                Workspace=f"_PDCal_diag_{groupID}",
            )
            self.mantidSnapper.CombineDiffCal(
                "Combine the new calibration values",
                PixelCalibration=self.calibrationTable,  # previous calibration values, DIFCprev
                GroupedCalibration=pdcalibratedWorkspace,  # values from PDCalibrate, DIFCpd
                CalibrationWorkspace=self.diffractionfocusedWStof,  # input WS to PDCalibrate, source for DIFCarb
                OutputWorkspace=self.calibrationTable,  # resulting corrected calibration values, DIFCeff
            )
            self.mantidSnapper.DeleteWorkspace(
                "Cleanup needless mask workspace",
                Workspace=pdcalibratedWorkspace + "_mask",
            )
            self.mantidSnapper.executeQueue()

        self.mantidSnapper.ApplyDiffCal(
            "Apply the new calibration constants",
            InstrumentWorkspace=self.inputWStof,
            CalibrationWorkspace=self.calibrationTable,
        )
        self.mantidSnapper.DeleteWorkspace(
            "Clean up pd group calibration table",
            Workspace=pdcalibratedWorkspace,
        )
        # this will re-process diffraction focused WS with new calibrations
        self.retrieveFromPantry()
        # save the data
        self.storeInPantry()
        self.setProperty("OutputWorkspace", self.diffractionfocusedWStof)
        self.setProperty("FinalCalibrationTable", self.calibrationTable)


# Register algorithm with Mantid
AlgorithmFactory.subscribe(GroupByGroupCalibration)
