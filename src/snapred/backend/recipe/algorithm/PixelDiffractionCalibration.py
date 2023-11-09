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


class PixelDiffractionCalibration(PythonAlgorithm):
    """
    Calculate the offset-corrected DIFC associated with a given workspace.
    One part of diffraction calibration.
    May be re-called iteratively with `execute` to ensure convergence.
    """

    def category(self):
        return "SNAPRed Diffraction Calibration"

    def PyInit(self):
        # declare properties
        self.declareProperty(
            MatrixWorkspaceProperty("InputWorkspace", "", Direction.Input, PropertyMode.Mandatory),
            doc="Workspace containing the raw neutron data",
        )
        self.declareProperty(
            MatrixWorkspaceProperty("GroupingWorkspace", "", Direction.Input, PropertyMode.Mandatory),
            doc="Workspace containing the grouping information",
        )
        self.declareProperty(
            MatrixWorkspaceProperty("OutputWorkspace", "", Direction.Output, PropertyMode.Optional),
            doc="Workspace containing the rebinned and pixel-calibrated data",
        )
        self.declareProperty(
            ITableWorkspaceProperty("CalibrationTable", "", Direction.Output, PropertyMode.Optional),
            doc="Workspace containing the rebinned and pixel-calibrated data",
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
        self.dBin: float = min([abs(d) for d in ingredients.focusGroup.dBin])
        self.dSpaceParams = (self.overallDMin, self.dBin, self.overallDMax)

        # from the instrument state, read the overall min/max TOF
        self.TOFMin: float = ingredients.instrumentState.particleBounds.tof.minimum
        self.TOFMax: float = ingredients.instrumentState.particleBounds.tof.maximum
        # instrConfig = ingredients.instrumentState.instrumentConfig
        self.TOFBin: float = self.dBin  # instrConfig.delTOverT / instrConfig.NBins
        self.TOFParams = (self.TOFMin, self.TOFBin, self.TOFMax)

        # from the grouped peak lists, find the maximum shift in d-spacing
        self.maxDSpaceShifts: Dict[int, float] = {}
        for peakList in ingredients.groupedPeakLists:
            self.maxDSpaceShifts[peakList.groupID] = 2.5 * peakList.maxfwhm

        # create string names of workspaces that will be used by algorithm
        if self.getProperty("OutputWorkspace").isDefault:
            self.outputWStof: str = self.getPropertyValue("InputWorkspace")
            self.setProperty("OutputWorkspace", self.outputWStof)
        else:
            self.outputWStof: str = self.getPropertyValue("OutputWorkspace")
        self.outputWSdsp: str = self.outputWStof + "_dsp"

        # create string name for output calibration table
        self.DIFCpixel: str = ""
        if self.getProperty("CalibrationTable").isDefault:
            self.DIFCpixel = f"_DIFC_{self.runNumber}"
            self.setProperty("CalibrationTable", self.DIFCpixel)
        else:
            self.DIFCpixel = self.getPropertyValue("CalibrationTable")

        # set the max offset
        self.maxOffset: float = ingredients.maxOffset

    def chopNeutronData(self) -> None:
        """
        Process input neutron data
        """

        self.wsRawTOF: str = self.getPropertyValue("InputWorkspace")

        # clone and crop the raw input data
        self.mantidSnapper.CropWorkspace(
            "Clone and crop the raw data",
            InputWorkspace=self.wsRawTOF,
            OutputWorkspace=self.outputWStof,
            XMin=self.TOFMin,
            Xmax=self.TOFMax,
        )
        # rebin the TOF data logarithmically
        self.convertUnitsAndRebin(self.outputWStof, self.outputWStof, "TOF")
        # also find d-spacing data and rebin logarithmically
        self.convertUnitsAndRebin(self.outputWStof, self.outputWSdsp, "dSpacing")

        # for inspection, make a copy of initial data
        inputWStof: str = self.getPropertyValue("InputWorkspace") + "_TOF_in"
        inputWSdsp: str = self.getPropertyValue("InputWorkspace") + "_DSP_in"
        self.mantidSnapper.MakeDirtyDish(
            "Creating copy of initial TOF data",
            InputWorkspace=self.outputWStof,
            OutputWorkspace=inputWStof,
        )
        self.mantidSnapper.MakeDirtyDish(
            "Creating copy of initial d-spacing data",
            InputWorkspace=self.outputWSdsp,
            OutputWorkspace=inputWSdsp,
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
            InputWorkspace=self.wsRawTOF,
            CalibrationTable=self.DIFCpixel,
            OffsetMode="Signed",
            BinWidth=self.TOFBin,
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

        data: Dict[str, float] = {}
        totalOffsetWS: str = f"offsets_{self.runNumber}_{self._counts}"
        wsoff: str = f"_{self.runNumber}_tmp_group_offset_{self._counts}"
        wscc: str = f"_{self.runNumber}_tmp_group_CC_{self._counts}"
        for groupID, workspaceIndices in self.groupWorkspaceIndices.items():
            workspaceIndices = list(workspaceIndices)
            refID: int = self.getRefID(workspaceIndices)
            self.mantidSnapper.CrossCorrelate(
                f"Cross-Correlating spectra for {wscc}",
                InputWorkspace=self.outputWSdsp,
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
        self.mantidSnapper.ApplyDiffCal(
            "Apply the diffraction calibration to the input TOF workspace",
            InstrumentWorkspace=self.outputWStof,
            CalibrationWorkspace=self.DIFCpixel,
        )
        # convert to d-spacing and rebin logarithmically
        self.convertUnitsAndRebin(self.outputWStof, self.outputWSdsp, "dSpacing")
        self.mantidSnapper.MakeDirtyDish(
            f"Store d-spacing data at {self._counts} iterations",
            InputWorkspace=self.outputWSdsp,
            OutputWorkspace=self.outputWSdsp + f"_{self._counts}",
        )

        # cleanup memory usage
        self.mantidSnapper.WashDishes(
            "Deleting temporary workspaces",
            WorkspaceList=[wscc, wsoff, totalOffsetWS, self.outputWSdsp],
        )
        # now execute the queue
        self.mantidSnapper.executeQueue()
        self.setProperty("data", json.dumps(data))
        self.setPropertyValue("OutputWorkspace", self.outputWStof)

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
            self.chopNeutronData()
        # now calculate and correct by offsets
        self.reexecute()
        self._counts += 1


# Register algorithm with Mantid
AlgorithmFactory.subscribe(PixelDiffractionCalibration)
