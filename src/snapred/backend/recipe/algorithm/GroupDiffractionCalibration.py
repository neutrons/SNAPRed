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

        # TODO setup for non-SNAPLite
        self.isLite: bool = True

        # from grouping parameters, read the overall min/max d-spacings
        self.dMax = ingredients.focusGroup.dMax
        self.dMin = ingredients.focusGroup.dMin
        self.dBin = [-abs(db) for db in ingredients.focusGroup.dBin]

        # from the instrument state, read the overall min/max TOF
        self.TOFMin: float = ingredients.instrumentState.particleBounds.tof.minimum
        self.TOFMax: float = ingredients.instrumentState.particleBounds.tof.maximum
        self.TOFBin: float = min([abs(db) for db in self.dBin])
        self.TOFParams = (self.TOFMin, -abs(self.TOFBin), self.TOFMax)

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

        if len(self.groupIDs) != ingredients.focusGroup.nHst:
            raise RuntimeError(
                f"Group IDs do not match between peak list and focus group: {self.groupIDs} vs {ingredients.focusGroup.nHst}"  # noqa: E501
            )

    def chopNeutronData(self):
        """
        Process input neutron data
        """

        self.rawWStof: str = self.getPropertyValue("InputWorkspace")
        self.focusWSname: str = str(self.getPropertyValue("GroupingWorkspace"))

        # set the previous calibration table, or create if none given
        self.DIFCprev: str = ""
        if self.getProperty("PreviousCalibrationTable").isDefault:
            self.DIFCprev = f"difc_prev_{self.runNumber}"
            self.mantidSnapper.CalculateDiffCalTable(
                "Initialize the DIFC table from input",
                InputWorkspace=self.rawWStof,
                CalibrationTable=self.DIFCprev,
                OffsetMode="Signed",
                BinWidth=self.TOFBin,
            )
        else:
            self.DIFCprev = str(self.getPropertyValue("PreviousCalibrationTable"))
        self.mantidSnapper.ApplyDiffCal(
            "Apply the diffraction calibration table to the input workspace",
            InstrumentWorkspace=self.rawWStof,
            CalibrationWorkspace=self.DIFCprev,
        )

        # se the final calibration table, to be the output
        self.DIFCfinal: str = ""
        if self.getProperty("FinalCalibrationTable").isDefault:
            self.DIFCfinal = self.DIFCprev
            self.setProperty("FinalCalibrationTable", self.DIFCfinal)
        else:
            self.DIFCfinal = str(self.getPropertyValue("PreviousCalibrationTable"))
            self.mantidSnapper.CloneWorkspace(
                "Make copy of calibration table, to not deform",
                InputWorkspace=self.DIFCprev,
                OutputWorkspace=self.DIFCfinal,
            )

        # create string names of workspaces that will be used by algorithm
        self.outputWStof: str = ""
        if self.getProperty("OutputWorkspace").isDefault:
            self.outputWStof = self.rawWStof + "_TOF_out"
            self.setProperty("OutputWorkspace", self.outputWStof)
        else:
            self.outputWStof = self.getPropertyValue("OutputWorkspace")

        # process and diffraction focus the input data
        # must convert to d-spacing, diffraction focus, ragged rebin, then convert back to TOF
        self.convertAndFocusAndReturn(self.rawWStof, self.outputWStof, "before")

    def PyExec(self) -> None:
        """
        Execute the group-by-group calibration algorithm.
        First a table of previous pixel calibrations must be loaded.
        Then, group-by-group, each spectrum is calibrated by fitting peaks, and the
        resulting DIFCs are combined with the previous table.
        The final calibration table is saved to disk for future use.
        input:
        - Ingredients: DiffractionCalibrationIngredients -- the DAO holding data needed to run the algorithm
        - InputWorkspace: str -- the name of workspace holding the initial TOF data
        - PreviousCalibrationTable: str -- the name of the table workspace with previous DIFC values
        output:
        - OutputWorkspace: str -- the name of the diffraction-focussed d-spacing data after the final calibration
        - FinalCalibrationTable: str -- the name of the final table of DIFC values
        """
        # run the algo
        self.log().notice("Execution of group diffraction calibration START!")

        # get the ingredients
        ingredients = Ingredients.parse_raw(self.getProperty("Ingredients").value)
        self.chopIngredients(ingredients)
        self.chopNeutronData()

        diffocWS = self.mantidSnapper.mtd[self.outputWStof]
        nHist = diffocWS.getNumberHistograms()
        if nHist != len(self.groupIDs):
            raise RuntimeError("error, the number of spectra in focused workspace, and number of groups, do not match")

        for index in range(nHist):
            groupID: int = self.groupIDs[index]
            DIFCpd: str = f"_tmp_PDCal_group_{groupID}"
            diagnosticWSgroup: str = f"_PDCal_diag_{groupID}"
            self.mantidSnapper.PDCalibration(
                f"Perform PDCalibration on group {groupID}",
                InputWorkspace=self.outputWStof,
                TofBinning=self.TOFParams,
                PeakFunction="Gaussian",
                BackgroundType="Linear",
                PeakPositions=self.groupedPeaks[groupID],
                PeakWindow=self.groupedPeakBoundaries[groupID],
                CalibrationParameters="DIFC",
                HighBackground=True,  # vanadium must use high background to FitPeaks
                OutputCalibrationTable=DIFCpd,
                DiagnosticWorkspaces=diagnosticWSgroup,
                # limit to specific spectrum
                StartWorkspaceIndex=index,
                StopWorkspaceIndex=index,
            )
            # self.mantidSnapper.ExtractSingleSpectrum(
            #     "Extract the calibrated spectrum's diagnostic workspace",
            #     InputWorkspace=diagnosticWSgroup,
            #     OutputWorkspace=diagnosticWSgroup,
            #     WorkspaceIndex = index,
            # )
            # if index == 0:
            #     self.mantidSnapper.CloneWorkspace(
            #         "Save the first diagnostic workspace",
            #         InputWorkspace = diagnosticWSgroup,
            #         OutputWorkspace = diagnosticWS
            #     )
            # else:
            #     self.mantidSnapper.ConjoinWorkspaces(
            #         "Combine diagnostic workspaces",
            #         InputWorkspace1=diagnosticWS,
            #         InputWorkspace2=diagnosticWSgroup,
            #     )
            self.mantidSnapper.CombineDiffCal(
                "Combine the new calibration values",
                PixelCalibration=self.DIFCprev,  # previous calibration values, DIFCprev
                GroupedCalibration=DIFCpd,  # values from PDCalibrate, DIFCpd
                CalibrationWorkspace=self.outputWStof,  # input WS to PDCalibrate, source for DIFCarb
                OutputWorkspace=self.DIFCfinal,  # resulting corrected calibration values, DIFCeff
            )
            self.mantidSnapper.WashDishes(
                "Cleanup leftover workspace",
                WorkspaceList=[DIFCpd, diagnosticWSgroup],
            )
            self.mantidSnapper.executeQueue()

        # apply the calibration table to input data, then re-focus
        self.mantidSnapper.ApplyDiffCal(
            "Apply the new calibration constants",
            InstrumentWorkspace=self.rawWStof,
            CalibrationWorkspace=self.DIFCfinal,
        )
        self.convertAndFocusAndReturn(self.rawWStof, self.outputWStof, "after")

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
