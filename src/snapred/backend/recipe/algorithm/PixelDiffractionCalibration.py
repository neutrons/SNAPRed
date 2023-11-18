import json
from typing import Dict, List, Tuple

import numpy as np
from mantid.api import (
    AlgorithmFactory,
    ITableWorkspaceProperty,
    MatrixWorkspaceProperty,
    PropertyMode,
    PythonAlgorithm,
)
from mantid.kernel import Direction

from snapred.backend.dao.ingredients import DiffractionCalibrationIngredients as Ingredients
from snapred.backend.recipe.algorithm.CalculateDiffCalTable import CalculateDiffCalTable
from snapred.backend.recipe.algorithm.MakeDirtyDish import MakeDirtyDish
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceNameGenerator as wng


class PixelDiffractionCalibration(PythonAlgorithm):
    """
    Calculate the offset-corrected DIFC associated with a given workspace.
    One part of diffraction calibration.
    May be re-called iteratively with `execute` to ensure convergence.
    """

    # NOTE: there is already a method for creating a copy of data from a clean version
    # therefore there is no reason not to deform the input workspace to this algorithm
    # instead, the original clean file is preserved in a separate location

    def category(self):
        return "SNAPRed Diffraction Calibration"

    def PyInit(self):
        # declare properties
        self.declareProperty(
            MatrixWorkspaceProperty("InputWorkspace", "", Direction.Input, PropertyMode.Mandatory),
            doc="Workspace containing the TOF neutron data",
        )
        self.declareProperty(
            MatrixWorkspaceProperty("GroupingWorkspace", "", Direction.Input, PropertyMode.Mandatory),
            doc="Workspace containing the grouping information",
        )
        self.declareProperty(
            ITableWorkspaceProperty("CalibrationTable", "", Direction.Output, PropertyMode.Optional),
            doc="Workspace containing the corrected calibration constants",
        )
        self.declareProperty("Ingredients", defaultValue="", direction=Direction.Input)  # noqa: F821
        self.declareProperty("data", defaultValue="", direction=Direction.Output)
        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, __name__)
        self._counts = 0

    # TODO: ensure all ingredients loaded elsewhere, no superfluous ingredients
    def chopIngredients(self, ingredients: Ingredients) -> None:
        """Receive the ingredients from the recipe, and exctract the needed pieces for this algorithm."""
        self.runNumber: str = ingredients.runConfig.runNumber

        # TODO setup for SNAPLite
        self.isLite: bool = False

        # from grouping parameters, read the overall min/max d-spacings
        self.overallDMin: float = min(ingredients.focusGroup.dMin)
        self.overallDMax: float = max(ingredients.focusGroup.dMax)
        self.dBin: float = max([abs(d) for d in ingredients.focusGroup.dBin])
        self.dSpaceParams = (self.overallDMin, self.dBin, self.overallDMax)

        # from the grouped peak lists, find the maximum shift in d-spacing
        self.maxDSpaceShifts: Dict[int, float] = {}
        for peakList in ingredients.groupedPeakLists:
            self.maxDSpaceShifts[peakList.groupID] = 2.5 * peakList.maxfwhm

        # create string name for output calibration table
        # TODO: use workspace namer
        self.DIFCpixel: str = ""
        if self.getProperty("CalibrationTable").isDefault:
            self.DIFCpixel = f"_DIFC_{self.runNumber}"
            self.setProperty("CalibrationTable", self.DIFCpixel)
        else:
            self.DIFCpixel = self.getPropertyValue("CalibrationTable")

        # set the max offset
        self.maxOffset: float = ingredients.maxOffset

    def unbagGroceries(self, ingredients: Ingredients) -> None:
        """
        Process input neutron data
        """

        self.wsTOF: str = self.getPropertyValue("InputWorkspace")
        # TODO: use workspace namer
        self.wsDSP: str = self.wsTOF + "_dsp"
        # for inspection, make a copy of initial data
        self.mantidSnapper.ConvertUnits(
            "Convert to d-spacing to diffraction focus",
            InputWorkspace=self.wsTOF,
            OutPutWorkspace=self.wsDSP,
            Target="dSpacing",
        )
        self.mantidSnapper.MakeDirtyDish(
            "Creating copy of initial TOF data",
            InputWorkspace=self.wsTOF,
            OutputWorkspace=self.wsTOF + "_pixelBegin",
        )
        self.mantidSnapper.MakeDirtyDish(
            "Creating copy of initial d-spacing data",
            InputWorkspace=self.wsDSP,
            OutputWorkspace=self.wsDSP + "_pixelBegin",
        )

        # get handle to group focusing workspace and retrieve all detector IDs
        focusWSname: str = str(self.getPropertyValue("GroupingWorkspace"))
        focusWS = self.mantidSnapper.mtd[focusWSname]
        self.groupIDs: List[int] = [int(x) for x in focusWS.getGroupIDs()]
        self.groupWorkspaceIndices: Dict[int, List[int]] = {}
        for groupID in self.groupIDs:
            groupDetectorIDs = [int(x) for x in focusWS.getDetectorIDsOfGroup(groupID)]
            self.groupWorkspaceIndices[groupID] = focusWS.getIndicesFromDetectorIDs(groupDetectorIDs)

        self.mantidSnapper.CalculateDiffCalTable(
            "Calculate initial table of DIFC values",
            InputWorkspace=self.wsTOF,
            CalibrationTable=self.DIFCpixel,
            OffsetMode="Signed",
            BinWidth=self.dBin,
        )

        if "EventWorkspace" in self.mantidSnapper.mtd[self.wsTOF].id():
            #############################################################
            # remove the background from the data
            # this helps with cross-correlation for high-background data
            # extracts peaks by deleting events inside the peak windows
            # then subtracts the peak-deleted workspace from the original
            #############################################################
            background: str = self.wsTOF + "_background"
            focusedBackground: str = background + "_focus"
            self.mantidSnapper.CloneWorkspace(
                "Create a copy to hold only the background",
                InputWorkspace=self.wsDSP,
                OutputWorkspace=background,
            )
            self.mantidSnapper.DiffractionFocussing(
                "Diffraction focus the background before stripping peaks",
                InputWorkspace=background,
                OutputWorkspace=focusedBackground,
                GroupingWorkspace=focusWSname,
            )
            self.mantidSnapper.executeQueue()
            # the background WS holds the original d-spacing data
            # each group should have similar d-spacing peaks
            # use the diffraction focused group to indicate all the detectors with these peaks
            # then at each of those detectors in full instrument, delete d spaces of peaks
            backgroundWS = self.mantidSnapper.mtd[background]
            focusedBackgroundWS = self.mantidSnapper.mtd[focusedBackground]
            nSpec = focusedBackgroundWS.getNumberHistograms()
            for index in range(nSpec):
                for detid in focusedBackgroundWS.getSpectrum(index).getDetectorIDs():
                    event_list = backgroundWS.getEventList(detid)
                    for peak in ingredients.groupedPeakList[index].peaks:
                        event_list.maskTof(peak.minimum, peak.maximum)
            # now subtract off the background and convert back to TOF
            self.mantidSnapper.Minus(
                "Subtract out the peak-stripped background",
                LHSWorkspace=self.wsDSP,
                RHSWorkspace=background,
                OutputWorkspace=self.wsDSP,
            )
            self.mantidSnapper.ConvertUnits(
                "Convert units back to TOF",
                InputWorkspace=self.wsDSP,
                OutputWorkspace=self.wsTOF,
                Target="TOF",
            )
            # for inspection, make a copy background-stripped data
            self.mantidSnapper.MakeDirtyDish(
                "Creating copy of initial TOF data",
                InputWorkspace=self.wsTOF,
                OutputWorkspace=self.wsTOF + "_pixelStripped",
            )
            self.mantidSnapper.MakeDirtyDish(
                "Creating copy of initial d-spacing data",
                InputWorkspace=self.wsDSP,
                OutputWorkspace=self.wsDSP + "_pixelStripped",
            )
            self.mantidSnapper.WashDishes(
                "Remove intermediate workspaces",
                WorkspaceList=[background, focusedBackground],
            )
        self.mantidSnapper.executeQueue()

    def convertUnitsAndRebin(self, inputWS: str, outputWS: str) -> None:
        """
        Convert units to target (either TOF or dSpacing) and then rebin logarithmically.
        If 'converting' from and to the same units, will only rebin.
        """
        self.mantidSnapper.ConvertUnits(
            "Converting to d-spacing",
            InputWorkspace=inputWS,
            OutputWorkspace=outputWS,
            Target="dSpacing",
        )
        self.mantidSnapper.Rebin(
            "Rebin the workspace logarithmically",
            InputWorkspace=outputWS,
            OutputWorkspace=outputWS,
            Params=self.dSpaceParams,
            BinningMode="Logarithmic",
        )
        self.mantidSnapper.executeQueue()

    # TODO: replace the median with some better method, to be determined
    # for choosing a reference pixel (and not using brightest pixel)
    def getRefID(self, detectorIDs: List[int]) -> int:
        """
        Calculate a unique reference pixel for a pixel grouping, based in the pixel geometry.
        input:
        - detectorIDs: List[int] -- a list of all of the detector IDs in that group
        output:
        - the median pixel ID (to be replaced with angular COM pixel)
        """
        return int(np.median(detectorIDs))

    def reexecute(self) -> None:
        """
        Execute the main algorithm, in a way that can be iteratively called.
        First the initial DIFC values must be calculated.  Then, group-by-group,
        the spectra are cross-correlated, the offsets calculated, and the original DIFC
        values are corrected by the offsets.
        outputs:
        - data: dict -- statistics of the offsets, for testing convergence
        - OuputWorkspace: str -- the name of the TOF data with new DIFCs applied
        - CalibrationTable: str -- the final table of DIFC values
        """

        self.log().notice(f"Executing pixel calibration iteration {self._counts}")
        # convert to d-spacing and rebin logarithmically
        self.convertUnitsAndRebin(self.wsTOF, self.wsDSP)

        data: Dict[str, float] = {}
        totalOffsetWS: str = f"offsets_{self.runNumber}_{self._counts}"
        totalCCWS: str = f"CC_{self.runNumber}_{self._counts}"
        wsoff: str = f"_{self.runNumber}_tmp_group_offset_{self._counts}"
        wscc: str = f"_{self.runNumber}_tmp_group_CC_{self._counts}"
        for groupID, workspaceIndices in self.groupWorkspaceIndices.items():
            workspaceIndices = list(workspaceIndices)
            refID: int = self.getRefID(workspaceIndices)
            self.mantidSnapper.CrossCorrelate(
                f"Cross-Correlating spectra for {wscc}",
                InputWorkspace=self.wsDSP,
                OutputWorkspace=wscc,
                ReferenceSpectra=refID,
                WorkspaceIndexList=workspaceIndices,
                XMin=self.overallDMin,
                XMax=self.overallDMax,
                MaxDSpaceShift=self.maxDSpaceShifts[groupID],
            )
            self.mantidSnapper.GetDetectorOffsets(
                f"Calculate offset workspace {wsoff}",
                InputWorkspace=wscc,
                OutputWorkspace=wsoff,
                XMin=-100,
                XMax=100,
                OffsetMode="Signed",
                MaxOffset=self.maxOffset,
            )
            # add in group offsets to total, or begin the sum if none
            if not self.mantidSnapper.mtd.doesExist(totalOffsetWS):
                self.mantidSnapper.CloneWorkspace(
                    f"Starting summation with offset workspace {wsoff}",
                    InputWorkspace=wsoff,
                    OutputWorkspace=totalOffsetWS,
                )
                self.mantidSnapper.executeQueue()
            else:
                self.mantidSnapper.Plus(
                    f"Adding in offset workspace {wsoff}",
                    LHSWorkspace=totalOffsetWS,
                    RHSWorkspace=wsoff,
                    OutputWorkspace=totalOffsetWS,
                )
            if not self.mantidSnapper.mtd.doesExist(totalCCWS):
                self.mantidSnapper.CloneWorkspace(
                    "Staring collection of cross-correlation workspaces",
                    InputWorkspace=wscc,
                    Outputworkspace=totalCCWS,
                )
            else:
                self.mantidSnapper.ConjoinWorkspaces(
                    "Combine cross-correlation workspaces",
                    InputWorkspace1=wscc,
                    InputWorkspace2=totalCCWS,
                )

        # offsets should converge to 0 with reexecution of the process
        # use the median, to avoid issues with possible pathologic pixels
        self.mantidSnapper.executeQueue()  # queue must run before ws in mantid data
        offsets = list(self.mantidSnapper.mtd[totalOffsetWS].extractY().ravel())
        offsets = [abs(x) for x in offsets]  # ignore negative
        data["medianOffset"] = abs(np.median(offsets))

        # get difcal corrected by offsets
        self.mantidSnapper.ConvertDiffCal(
            "Correct previous calibration constants by offsets",
            OffsetsWorkspace=totalOffsetWS,
            PreviousCalibration=self.DIFCpixel,
            OutputWorkspace=self.DIFCpixel,
            OffsetMode="Signed",
            BinWidth=self.dBin,
        )

        # apply offset correction to input workspace
        # this improves the unit conversion in next iteration
        self.mantidSnapper.ApplyDiffCal(
            "Apply the diffraction calibration to the input TOF workspace",
            InstrumentWorkspace=self.wsTOF,
            CalibrationWorkspace=self.DIFCpixel,
        )
        self.mantidSnapper.MakeDirtyDish(
            f"Store d-spacing data at {self._counts} iterations",
            InputWorkspace=self.wsDSP,
            OutputWorkspace=self.wsDSP + f"_pixel{self._counts}",
        )

        # cleanup memory usage
        self.mantidSnapper.WashDishes(
            "Deleting temporary workspaces",
            WorkspaceList=[wscc, wsoff, totalOffsetWS, totalCCWS, self.wsDSP, "Mask"],
        )
        # now execute the queue
        self.mantidSnapper.executeQueue()
        self.setProperty("data", json.dumps(data))

    def PyExec(self) -> None:
        """
        Calculate pixel calibration DIFC values on each spectrum group.
        inputs:
        - Ingredients: DiffractionCalibrationIngredients -- the DAO holding data needed to run the algorithm
        - InputWorkspace: str -- the raw neutron data to be processed
        - GroupingWorkspace: str -- a pixel grouping workspace
        outputs:
        - data: dict -- several statistics of the offsets, for testing convergence
        - OuputWorkspace: str -- the name of the TOF data with new DIFCs applied
        - CalibrationTable: str -- the final table of DIFC values
        """
        # run the algo
        self.log().notice("Execution of pixel diffraction calibration START!")

        if self._counts == 0:
            self.log().notice("Extraction of calibration constants START!")
            # get the ingredients
            ingredients = Ingredients.parse_raw(self.getPropertyValue("Ingredients"))
            self.chopIngredients(ingredients)
            # load and process the input data for algorithm
            self.unbagGroceries(ingredients)
        # now calculate and correct by offsets
        self.reexecute()
        self._counts += 1


# Register algorithm with Mantid
AlgorithmFactory.subscribe(PixelDiffractionCalibration)
