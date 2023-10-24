import json
from typing import Dict, List, Tuple

from mantid.api import (
    AlgorithmFactory,
    ITableWorkspaceProperty,
    MatrixWorkspaceProperty,
    PropertyMode,
    PythonAlgorithm,
)
from mantid.kernel import Direction

from snapred.backend.dao.ingredients import DiffractionCalibrationIngredients as Ingredients
from snapred.backend.recipe.algorithm.MakeDirtyDish import MakeDirtyDish
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper


class GroupDiffractionCalibration(PythonAlgorithm):
    """
    Calculuate the group-aligned DIFC associated with a given workspace.
    One part of diffraction calibration.
    """

    def category(self):
        return "SNAPRed Diffraction Calibration"

    def PyInit(self):
        # declare properties
        self.declareProperty(
            MatrixWorkspaceProperty("InputWorkspace", "", Direction.Input, PropertyMode.Mandatory),
            doc="Workspace containing neutron data that has been pixel calibrated",
        )
        self.declareProperty(
            MatrixWorkspaceProperty("GroupingWorkspace", "", Direction.Input, PropertyMode.Mandatory),
            doc="Workspace containing the grouping information",
        )
        self.declareProperty(
            ITableWorkspaceProperty("PreviousCalibrationTable", "", Direction.Input, PropertyMode.Optional),
            doc="Table workspace with previous pixel-calibrated DIFC values",
        )
        self.declareProperty(
            MatrixWorkspaceProperty("OutputWorkspace", "", Direction.Output, PropertyMode.Optional),
            doc="Workspace containing the final diffraction calibration data",
        )
        self.declareProperty(
            ITableWorkspaceProperty("FinalCalibrationTable", "", Direction.Output, PropertyMode.Optional),
            doc="Table workspace with group-corrected DIFC values",
        )
        self.declareProperty("Ingredients", defaultValue="", direction=Direction.Input)  # noqa: F821
        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, __name__)

    def chopIngredients(self, ingredients: Ingredients) -> None:
        """Receive the ingredients from the recipe, and exctract the needed pieces for this algorithm."""
        from datetime import date

        """Receive the ingredients from the recipe, and exctract the needed pieces for this algorithm."""
        self.runNumber: str = ingredients.runConfig.runNumber

        # TODO setup for SNAPLite
        self.isLite: bool = False

        # from the instrument state, read the overall min/max TOF
        self.TOFMin: float = ingredients.instrumentState.particleBounds.tof.minimum
        self.TOFMax: float = ingredients.instrumentState.particleBounds.tof.maximum
        instrConfig = ingredients.instrumentState.instrumentConfig
        self.TOFBin: float = instrConfig.delTOverT / instrConfig.NBins
        self.TOFParams = (self.TOFMin, self.TOFBin, self.TOFMax)

        # from grouping parameters, read the overall min/max d-spacings
        self.overallDMin: float = max(ingredients.focusGroup.dMin)
        self.overallDMax: float = min(ingredients.focusGroup.dMax)
        self.dBin: float = abs(min(ingredients.focusGroup.dBin))
        self.dSpaceParams = (self.overallDMin, self.dBin, self.overallDMax)

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

    def chopNeutronData(self):
        """
        Process input neutron data
        """
        # create string names of workspaces that will be used by algorithm
        self.wsRawTOF: str = self.getPropertyValue("InputWorkspace")
        self.inputWStof: str = self.wsRawTOF + "_TOF_in"
        self.inputWSdsp: str = self.wsRawTOF + "_DSP_in"

        if self.getProperty("OutputWorkspace").isDefault:
            self.outputWStof: str = self.inputWStof
        else:
            self.outputWStof: str = self.getPropertyValue("OutputWorkspace")
        self.outputWSdsp: str = self.outputWStof + "_dsp"

        # set the previous calibration table, or create if none given
        if self.getProperty("PreviousCalibrationTable").isDefault:
            self.calibrationTable: str = f"difc_{self.runNumber}"
            self.mantidSnapper.CalculateDiffCalTable(
                "Initialize the DIFC table from input",
                InputWorkspace=self.wsRawTOF,
                CalibrationTable=self.calibrationTable,
                OffsetMode="Signed",
                BinWidth=self.TOFBin,
            )
        else:
            self.calibrationTable: str = str(self.getPropertyValue("PreviousCalibrationTable"))

        if self.getProperty("FinalCalibrationTable").isDefault:
            self.outputCalibrationTable: str = self.calibrationTable
        else:
            self.outputCalibrationTable: str = str(self.getPropertyValue("PreviousCalibrationTable"))
            self.mantidSnapper.CloneWorkspace(
                "Make copy of calibration table, to not deform",
                InputWorkspace=self.calibrationTable,
                OutputWorkspace=self.outputCalibrationTable,
            )

        self.focusWSname: str = str(self.getPropertyValue("GroupingWorkspace"))

        # process and diffraction focus the input data
        # must rebin, convert units, rebin, then diffraction focus
        self.convertUnitsAndRebin(self.wsRawTOF, self.inputWStof, "TOF")
        self.convertUnitsAndRebin(self.wsRawTOF, self.inputWSdsp, "dSpacing")
        self.mantidSnapper.DiffractionFocussing(
            "Refocus with offset-corrections",
            InputWorkspace=self.inputWSdsp,
            GroupingWorkspace=self.focusWSname,
            OutputWorkspace=self.outputWSdsp,
        )
        self.mantidSnapper.WashDishes(
            "Clean up unused d-spacing input",
            Workspace=self.inputWSdsp,
        )
        self.convertUnitsAndRebin(self.outputWSdsp, self.outputWStof, "TOF")

        # for inspection, save diffraction focused data before calculation
        self.mantidSnapper.MakeDirtyDish(
            "save diffraction-focused TOF data",
            InputWorkspace=self.outputWStof,
            OutputWorkspace=f"_TOF_{self.runNumber}_diffoc_before",
        )
        self.mantidSnapper.MakeDirtyDish(
            "save diffraction-focused d-spacing data",
            InputWorkspace=self.outputWSdsp,
            OutputWorkspace=f"_DSP_{self.runNumber}_diffoc_before",
        )

        self.mantidSnapper.executeQueue()

    def convertUnitsAndRebin(self, inputWS: str, outputWS: str, target: str = "dSpacing") -> None:
        """
        Convert units to target (either TOF or dSpacing) and then rebin logarithmically.
        If 'converting' from and to the same units, will only rebin.
        """
        self.mantidSnapper.ConvertUnits(
            f"Convert units to {target}",
            InputWorkspace=inputWS,
            OutputWorkspace=outputWS,
            Target=target,
        )

        rebinParams: Tuple[float, float, float]
        if target == "dSpacing":
            rebinParams = self.dSpaceParams
        elif target == "TOF":
            rebinParams = self.TOFParams
        self.mantidSnapper.Rebin(
            "Rebin the workspace logarithmically",
            InputWorkspace=outputWS,
            OutputWorkspace=outputWS,
            Params=rebinParams,
            BinningMode="Logarithmic",
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
        self.log().notice("Execution of group diffraction calibration START!")

        # get the ingredients
        ingredients = Ingredients.parse_raw(self.getProperty("Ingredients").value)
        self.chopIngredients(ingredients)
        self.chopNeutronData()

        pdcalibratedWorkspace = "_tmp_PDCal_subgroup"

        diffocWS = self.mantidSnapper.mtd[self.outputWStof]
        nHist = diffocWS.getNumberHistograms()
        if nHist != len(self.groupIDs):
            raise RuntimeError("error, the number of spectra in focused workspace, and number of groups, do not match")

        for index in range(nHist):
            groupID = self.groupIDs[index]
            self.mantidSnapper.PDCalibration(
                f"Perform PDCalibration on subgroup {groupID}",
                InputWorkspace=self.outputWStof,
                TofBinning=(self.TOFMin, -abs(self.TOFBin), self.TOFMax),
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
            self.mantidSnapper.MakeDirtyDish(
                "Save this group's calibrated WS",
                InputWorkspace=pdcalibratedWorkspace,
                OutputWorkspace=pdcalibratedWorkspace + f"_{groupID}",
            )
            self.mantidSnapper.WashDishes(
                "Cleanup needless diagnostic workspace",
                Workspace=f"_PDCal_diag_{groupID}",
            )
            self.mantidSnapper.CombineDiffCal(
                "Combine the new calibration values",
                PixelCalibration=self.calibrationTable,  # previous calibration values, DIFCprev
                GroupedCalibration=pdcalibratedWorkspace,  # values from PDCalibrate, DIFCpd
                CalibrationWorkspace=self.outputWStof,  # input WS to PDCalibrate, source for DIFCarb
                OutputWorkspace=self.outputCalibrationTable,  # resulting corrected calibration values, DIFCeff
            )
            self.mantidSnapper.executeQueue()

        self.mantidSnapper.WashDishes(
            "Clean up pd group calibration table",
            Workspace=pdcalibratedWorkspace,
        )

        # apply the calibration table to input data, then re-focus
        self.mantidSnapper.ApplyDiffCal(
            "Apply the new calibration constants",
            InstrumentWorkspace=self.inputWStof,
            CalibrationWorkspace=self.outputCalibrationTable,
        )
        self.convertUnitsAndRebin(self.inputWStof, self.outputWSdsp, "dSpacing")
        self.mantidSnapper.DiffractionFocussing(
            "Diffraction focus with final calibrated values",
            InputWorkspace=self.outputWSdsp,
            GroupingWorkspace=self.focusWSname,
            OutputWorkspace=self.outputWSdsp,
        )
        self.convertUnitsAndRebin(self.outputWSdsp, self.outputWStof, "TOF")


# Register algorithm with Mantid
AlgorithmFactory.subscribe(GroupDiffractionCalibration)
