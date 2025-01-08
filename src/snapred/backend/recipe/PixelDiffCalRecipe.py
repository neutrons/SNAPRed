from typing import Any, Dict, List, Set

import numpy as np
from pydantic import BaseModel

from snapred.backend.dao.ingredients import DiffractionCalibrationIngredients as Ingredients
from snapred.backend.log.logger import snapredLogger
from snapred.backend.recipe.algorithm.Utensils import Utensils
from snapred.backend.recipe.Recipe import Recipe, WorkspaceName
from snapred.meta.Config import Config
from snapred.meta.mantid.WorkspaceNameGenerator import WorkspaceNameGenerator as wng

logger = snapredLogger.getLogger(__name__)


class PixelDiffCalServing(BaseModel):
    result: bool
    medianOffsets: List[float]
    maskWorkspace: str
    calibrationTable: str


class PixelDiffCalRecipe(Recipe[Ingredients]):
    """
    Calculate the offset-corrected DIFC associated with a given workspace.
    One part of diffraction calibration.
    May be re-called iteratively with `execute` to ensure convergence.
    """

    # NOTE: there is already a method for creating a copy of data from a clean version
    # therefore there is no reason not to deform the input workspace to this algorithm
    # instead, the original clean file is preserved in a separate location

    MAX_DSPACE_SHIFT_FACTOR = Config["calibration.diffraction.maxDSpaceShiftFactor"]

    def __init__(self, utensils: Utensils = None):
        if utensils is None:
            utensils = Utensils()
            utensils.PyInit()
        self.mantidSnapper = utensils.mantidSnapper
        self._counts = 0

    def logger(self):
        return logger

    def mandatoryInputWorkspaces(self) -> Set[str]:
        return {"inputWorkspace", "groupingWorkspace"}

    def chopIngredients(self, ingredients: Ingredients) -> None:
        """Receive the ingredients from the recipe, and exctract the needed pieces for this algorithm."""
        self.runNumber: str = ingredients.runConfig.runNumber

        # from grouping parameters, read the overall min/max d-spacings
        dMin: List[float] = ingredients.pixelGroup.dMin()
        dMax: List[float] = ingredients.pixelGroup.dMax()
        dBin: List[float] = ingredients.pixelGroup.dBin()
        self.overallDMin: float = min(dMin)
        self.overallDMax: float = max(dMax)
        self.dBin: float = min([abs(d) for d in dBin])
        self.dSpaceParams = (self.overallDMin, self.dBin, self.overallDMax)
        self.tofParams = ingredients.pixelGroup.timeOfFlight.params
        self.removeBackground = ingredients.removeBackground
        self.detectorPeaks = ingredients.groupedPeakLists
        self.threshold = ingredients.convergenceThreshold
        self.maxIterations = Config["calibration.diffraction.maximumIterations"]
        # from the grouped peak lists, find the maximum shift in d-spacing
        self.maxDSpaceShifts: Dict[int, float] = {}
        for peakList in ingredients.groupedPeakLists:
            self.maxDSpaceShifts[peakList.groupID] = self.MAX_DSPACE_SHIFT_FACTOR * peakList.maxfwhm

        # set the max offset
        self.maxOffset: float = ingredients.maxOffset
        # keep track of previous median offsets
        self.medianOffsets = []

    def unbagGroceries(self, groceries: Dict[str, WorkspaceName]) -> None:  # noqa ARG002
        """
        Process input neutron data
        """
        self.wsTOF = groceries["inputWorkspace"]
        self.groupingWS = groceries["groupingWorkspace"]
        self.maskWS = groceries["maskWorkspace"]
        # the name of the output calibration table
        self.DIFCpixel = groceries["calibrationTable"]
        self.DIFCprev = groceries.get("previousCalibration", "")
        # the input data converted to d-spacing
        self.wsDSP = wng.diffCalInputDSP().runNumber(self.runNumber).build()
        self.convertUnitsAndRebin(self.wsTOF, self.wsDSP)
        self.mantidSnapper.CloneWorkspace(
            "Creating copy of initial d-spacing data",
            InputWorkspace=self.wsDSP,
            OutputWorkspace=self.wsDSP + "_beforeCrossCor",
        )

        if self.removeBackground:
            self.stripBackground(self.detectorPeaks, self.wsTOF, self.groupingWS)
            self.convertUnitsAndRebin(self.wsTOF, self.wsDSP)
            self.mantidSnapper.MakeDirtyDish(
                "Creating copy of background-subtracted d-spacing",
                InputWorkspace=self.wsDSP,
                OutputWorkspace=self.wsDSP + "_withoutBackground",
            )

        if self.DIFCprev == "":
            self.mantidSnapper.CalculateDiffCalTable(
                "Calculate initial table of DIFC values",
                InputWorkspace=self.wsTOF,
                CalibrationTable=self.DIFCpixel,
                OffsetMode="Signed",
                BinWidth=self.dBin,
            )
        else:
            self.mantidSnapper.CloneWorkspace(
                "Begin DIFC table at previous",
                InputWorkspace=self.DIFCprev,
                OutputWorkspace=self.DIFCpixel,
            )

    def stirInputs(self):
        self.groupWorkspaceIndices = self.mantidSnapper.GroupedDetectorIDs(
            "Extract the detector IDs for each group",
            GroupingWorkspace=self.groupingWS,
        )
        self.mantidSnapper.executeQueue()

    def stripBackground(self, peaks: List[Any], inputWS: WorkspaceName, groupingWS: WorkspaceName):
        self.mantidSnapper.Rebin(
            "Rebin thedata before removing baackground",
            InputWorkspace=inputWS,
            OutputWorkspace=inputWS,
            Params=self.tofParams,
            BinningMode="Logarithmic",
        )
        self.mantidSnapper.RemoveSmoothedBackground(
            "Extracting smoothed background from input data",
            InputWorkspace=inputWS,
            OutputWorkspace=inputWS,
            GroupingWorkspace=groupingWS,
            DetectorPeaks=peaks,
        )

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
            PreserveEvents=not self.removeBackground,
            BinningMode="Logarithmic",
        )

    # TODO: replace the median with some better method, to be determined
    #   for choosing a reference pixel (and not using brightest pixel)
    def getRefID(self, detectorIDs: List[int]) -> int:
        """
        Calculate a unique reference pixel for a pixel grouping, based on the pixel geometry.
        input:
        - detectorIDs: List[int] -- a list of all of the detector IDs in that group.
        output:
        - the median pixel ID (to be replaced with angular COM pixel).
        """
        # For the present method: either detector-ids or detector workspace indices may be used as input.
        return sorted(detectorIDs)[int(np.round((len(detectorIDs) - 1) / 2.0))]

    def queueAlgos(self):
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

        self.logger().info(f"Executing pixel calibration iteration {self._counts}")
        # convert to d-spacing and rebin logarithmically
        self.convertUnitsAndRebin(self.wsTOF, self.wsDSP)

        self.totalOffsetWS: str = f"offsets_{self.runNumber}_{self._counts}"
        wsoff: str = f"_{self.runNumber}_tmp_group_offset_{self._counts}"
        wscc: str = f"_{self.runNumber}_tmp_group_CC_{self._counts}"

        for i, (groupID, workspaceIndices) in enumerate(self.groupWorkspaceIndices.items()):
            workspaceIndices = list(workspaceIndices)
            refID: int = self.getRefID(workspaceIndices)

            self.mantidSnapper.CrossCorrelate(
                f"Cross-Correlating spectra for {wscc}",
                InputWorkspace=self.wsDSP,
                OutputWorkspace=wscc + f"_group{groupID}",
                ReferenceSpectra=refID,
                WorkspaceIndexList=workspaceIndices,
                XMin=self.overallDMin,
                XMax=self.overallDMax,
                MaxDSpaceShift=self.maxDSpaceShifts[groupID],
            )

            self.mantidSnapper.GetDetectorOffsets(
                f"Calculate offset workspace {wsoff}",
                InputWorkspace=wscc + f"_group{groupID}",
                OutputWorkspace=wsoff,
                MaskWorkspace=self.maskWS,
                # Scale the fitting ROI using the expected peak width (including a few possible peaks):
                XMin=-(self.maxDSpaceShifts[groupID] / self.dBin) * 2.0,
                XMax=(self.maxDSpaceShifts[groupID] / self.dBin) * 2.0,
                OffsetMode="Signed",
                MaxOffset=self.maxOffset,
            )

            # add in group offsets to total, or begin the sum if none
            # NOTE wsoff has all spectra, with value 0 in those not used in CrossCorrelate
            if i == 0:
                self.mantidSnapper.CloneWorkspace(
                    f"Starting summation with offset workspace {wsoff}",
                    InputWorkspace=wsoff,
                    OutputWorkspace=self.totalOffsetWS,
                )
            else:
                self.mantidSnapper.Plus(
                    f"Adding in offset workspace {wsoff}",
                    LHSWorkspace=self.totalOffsetWS,
                    RHSWorkspace=wsoff,
                    OutputWorkspace=self.totalOffsetWS,
                )
            self.mantidSnapper.WashDishes(
                "Remove the temporary cross-correlation workspace",
                WorkspaceList=[wscc + f"_group{groupID}", wsoff],
            )

        # cleanup memory usage

        self.mantidSnapper.WashDishes(
            "Deleting temporary workspaces",
            Workspace=self.wsDSP,
        )

    def prep(self, ingredients: Ingredients, groceries: Dict[str, str]):
        """
        Convenience method to prepare the recipe for execution.
        """
        self.validateInputs(ingredients, groceries)
        self.chopIngredients(ingredients)
        self.unbagGroceries(groceries)
        self.stirInputs()

        self.queueAlgos()

    def execute(self):
        # Offsets should converge to zero with re-execution of the process.
        # Testing uses the median, to avoid issues with possible pathologic pixels.
        offsetStats = self.mantidSnapper.OffsetStatistics(
            "Get the median, mean, etc. for the offsets",
            OffsetsWorkspace=self.totalOffsetWS,
        )
        self.mantidSnapper.executeQueue()

        newOffset = offsetStats["medianOffset"]

        if newOffset <= (self.medianOffsets[-1:] or (self.maxOffset * 10,))[0]:
            self.medianOffsets.append(newOffset)
            # get difcal corrected by offsets
            self.mantidSnapper.ConvertDiffCal(
                "Correct previous calibration constants by offsets",
                OffsetsWorkspace=self.totalOffsetWS,
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
        else:
            logger.warning("Offsets failed to converge monotonically")

        self.mantidSnapper.MakeDirtyDish(
            f"Store d-spacing data at {self._counts} iterations",
            InputWorkspace=self.wsDSP,
            OutputWorkspace=self.wsDSP + f"_pixel{self._counts}",
        )
        self.mantidSnapper.WashDishes(
            "Clean up memory usage",
            WorkspaceList=[self.wsDSP, self.totalOffsetWS],
        )
        self.mantidSnapper.executeQueue()
        # return
        return self.medianOffsets[-1] > self.threshold and newOffset <= self.medianOffsets[-1]

    def cook(self, ingredients: Ingredients, groceries: Dict[str, str]) -> Dict[str, Any]:
        """
        Main interface for the recipe.
        Will call the execution iteratively until convergence is reached.
        """
        self.prep(ingredients, groceries)
        while self.execute():
            self._counts = self._counts + 1
            if self._counts >= self.maxIterations:
                logger.warning("Offset convergence reached max iterations without converging")
                break
            logger.info(f"... converging to answer; step {self._counts}, {self.medianOffsets[-1]} > {self.threshold}")
            self.queueAlgos()
        logger.info(f"Pixel calibration converged.  Offsets: {self.medianOffsets}")

        # create for inspection
        self.convertUnitsAndRebin(self.wsTOF, f"{self.wsDSP}_afterCrossCor")
        self.mantidSnapper.executeQueue()

        return PixelDiffCalServing(
            result=True,
            medianOffsets=self.medianOffsets,
            calibrationTable=self.DIFCpixel,
            maskWorkspace=self.maskWS,
        )
