import json
from typing import Dict, List, Tuple

from mantid.api import AlgorithmFactory, PythonAlgorithm
from mantid.kernel import Direction

from snapred.backend.dao.ingredients import DiffractionCalibrationIngredients as Ingredients
from snapred.backend.recipe.algorithm.LoadGroupingDefinition import LoadGroupingDefinition
from snapred.backend.recipe.algorithm.MakeDirtyDish import MakeDirtyDish
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper


class GroupDiffractionCalibration(PythonAlgorithm):
    def category(self):
        return "SNAPRed Diffraction Calibration"

    def PyInit(self):
        # declare properties
        self.declareProperty("Ingredients", defaultValue="", direction=Direction.Input)  # noqa: F821
        self.declareProperty("InputWorkspace", defaultValue="", direction=Direction.Input)
        self.declareProperty("PreviousCalibrationTable", defaultValue="", direction=Direction.Input)
        self.declareProperty("OutputWorkspace", defaultValue="", direction=Direction.Output)
        self.declareProperty("FinalCalibrationTable", defaultValue="", direction=Direction.Output)
        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, __name__)

    def chopIngredients(self, ingredients: Ingredients) -> None:
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

        # from grouping parameters, read the overall min/max d-spacings
        self.dMax: List[float] = ingredients.focusGroup.dMin
        self.dMin: List[float] = ingredients.focusGroup.dMax
        self.dBin: List[float] = ingredients.focusGroup.dBin

        # from the instrument state, read the overall min/max TOF
        self.TOFMin: float = ingredients.instrumentState.particleBounds.tof.minimum
        self.TOFMax: float = ingredients.instrumentState.particleBounds.tof.maximum
        self.TOFBin: float = min([abs(db) for db in self.dBin])
        self.TOFParams = (self.TOFMin, -abs(self.TOFBin), self.TOFMax)

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

    def raidPantry(self):
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

        # now diffraction focus the d-spacing data and convert to TOF
        self.focusWSname = f"_{self.runNumber}_focusGroup"
        self.mantidSnapper.LoadGroupingDefinition(
            f"Loading grouping file {self.groupingFile}...",
            GroupingFilename=self.groupingFile,
            InstrumentDonor=self.inputWStof,
            OutputWorkspace=self.focusWSname,
        )
        self.convertAndFocusAndReturn(self.inputWStof, self.diffractionfocusedWStof, "before")

    def restockPantry(self) -> None:
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
        ingredients = Ingredients.parse_raw(self.getProperty("Ingredients").value)
        self.chopIngredients(ingredients)
        self.raidPantry()

        pdcalibratedWorkspace = "_tmp_PDCal_subgroup"

        diffocWS = self.mantidSnapper.mtd[self.diffractionfocusedWStof]
        nHist = diffocWS.getNumberHistograms()
        if nHist != len(self.groupIDs):
            raise RuntimeError("error, the number of spectra in focused workspace, and number of groups, do not match")

        # remove overlapping peaks

        for index in range(nHist):
            groupID = self.groupIDs[index]
            self.mantidSnapper.PDCalibration(
                f"Perform PDCalibration on subgroup {groupID}",
                InputWorkspace=self.diffractionfocusedWStof,
                TofBinning=self.TOFParams,
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
            self.mantidSnapper.WashDishes(
                "Cleanup needless diagnostic workspace",
                Workspace=f"_PDCal_diag_{groupID}",
            )
            self.mantidSnapper.CombineDiffCal(
                "Combine the new calibration values",
                PixelCalibration=self.calibrationTable,  # previous calibration values, DIFCprev
                GroupedCalibration=pdcalibratedWorkspace,  # values from PDCalibrate, DIFCpd
                CalibrationWorkspace=self.diffractionfocusedWStof,  # input WS to PDCalibrate, source for DIFCarb
                OutputWorkspace=self.calibrationTable,  # resulting corrected calibration values, DIFCeff
            )
            self.mantidSnapper.WashDishes(
                "Cleanup needless mask workspace",
                Workspace=pdcalibratedWorkspace + "_mask",
            )
            self.mantidSnapper.executeQueue()

        self.mantidSnapper.ApplyDiffCal(
            "Apply the new calibration constants",
            InstrumentWorkspace=self.inputWStof,
            CalibrationWorkspace=self.calibrationTable,
        )
        self.convertAndFocusAndReturn(self.inputWStof, self.diffractionfocusedWStof, "after")

        # save the data
        self.setProperty("OutputWorkspace", self.diffractionfocusedWStof)
        self.setProperty("FinalCalibrationTable", self.calibrationTable)
        self.restockPantry()

    def convertAndFocusAndReturn(self, inputWS: str, outputWS: str, note: str):
        tmpWStof = f"_TOF_{self.runNumber}_diffoc_{note}"
        tmpWSdsp = f"_DSP_{self.runNumber}_diffoc_{note}"
        self.mantidSnapper.ConvertUnits(
            "Convert thr raw TOf data to d-spacing",
            InputWorkspace=inputWS,
            OutputWorkspace=tmpWSdsp,
            Target="dSpacing",
        )
        self.mantidSnapper.DiffractionFocussing(
            "Diffraction-focus the d-spacing data",
            InputWorkspace=tmpWSdsp,
            GroupingWorkspace=self.focusWSname,
            OutputWorkspace=tmpWSdsp,
        )
        self.mantidSnapper.RebinRagged(
            "Ragged rebin the diffraction-focused data",
            InputWorkspace=tmpWSdsp,
            OutputWorkspace=tmpWSdsp,
            XMin=self.dMin,
            XMax=self.dMax,
            Delta=self.dBin,
        )
        self.mantidSnapper.ConvertUnits(
            "Convert the focused and rebined data back to TOF",
            InputWorkspace=tmpWSdsp,
            OutputWorkspace=outputWS,
            Target="TOF",
        )
        # for inspection, save diffraction focused data before calculation
        self.mantidSnapper.MakeDirtyDish(
            "save diffraction-focused TOF data",
            InputWorkspace=outputWS,
            OutputWorkspace=tmpWStof,
        )
        self.mantidSnapper.WashDishes(
            "save diffraction-focused d-spacing data",
            Workspace=tmpWSdsp,
        )
        self.mantidSnapper.executeQueue()


# Register algorithm with Mantid
AlgorithmFactory.subscribe(GroupDiffractionCalibration)
