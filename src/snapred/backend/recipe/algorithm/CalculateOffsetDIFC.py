import json
from typing import Dict, List

import numpy as np
from mantid.api import AlgorithmFactory, PythonAlgorithm
from mantid.kernel import Direction

from snapred.backend.dao.DiffractionCalibrationIngredients import DiffractionCalibrationIngredients
from snapred.backend.recipe.algorithm.ConvertDiffCalLog import ConvertDiffCalLog  # noqa
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper

name = "CalculateOffsetDIFC"


class CalculateOffsetDIFC(PythonAlgorithm):
    """
    Calculate the offset-corrected DIFC associated with a given workspace.
    One part of diffraction calibration.
    After being called by `execute`, may be called iteratively with `reexecute` to ensure convergence.
    """

    def PyInit(self):
        # declare properties
        self.declareProperty("Ingredients", defaultValue="", direction=Direction.Input)  # noqa: F821
        self.declareProperty("CalibrationTable", defaultValue="", direction=Direction.Output)
        self.declareProperty("OutputWorkspace", defaultValue="", direction=Direction.Output)
        self.declareProperty("data", defaultValue="", direction=Direction.Output)
        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, name)

    # TODO: ensure all ingredients loaded elsewhere, no superfluous ingredients
    def chopIngredients(self, ingredients: DiffractionCalibrationIngredients) -> None:
        """Receive the ingredients from the recipe, and exctract the needed pieces for this algorithm."""
        self.runNumber: str = ingredients.runConfig.runNumber
        self.ipts: str = ingredients.runConfig.IPTS
        self.rawDataPath: str = self.ipts + "shared/lite/SNAP_{}.lite.nxs.h5".format(ingredients.runConfig.runNumber)

        # TODO setup for SNAPLite
        self.isLite: bool = False

        # fdrom the instrument state, read the overall min/max TOF
        self.TOFMin: float = ingredients.instrumentState.particleBounds.tof.minimum
        self.TOFMax: float = ingredients.instrumentState.particleBounds.tof.maximum
        self.TOFBin: float = -0.001

        # from grouping parameters, read the overall min/max d-spacings
        self.overallDMin: float = max(ingredients.focusGroup.dMin)
        self.overallDMax: float = min(ingredients.focusGroup.dMax)
        self.dBin: float = -min(
            [abs(x) for x in ingredients.focusGroup.dBin]
        )  # ensure dBin is negative for log binning
        self.maxDSpaceShifts: float = 2.5 * max(ingredients.focusGroup.FWHM)

        # path to grouping file, specifying group IDs of pixels
        self.groupingFile: str = ingredients.focusGroup.definition

        # create string names of workspaces that will be used by algorithm
        self.inputWStof: str = f"_TOF_{self.runNumber}_raw"
        self.inputWSdsp: str = f"_DSP_{self.runNumber}_raw"
        self.difcWS: str = f"_DIFC_{self.runNumber}"

    def retrieveFromPantry(self) -> None:
        """Initialize the input TOF data from the input filename in the ingredients"""
        if not self.mantidSnapper.mtd.doesExist(self.inputWStof):
            self.mantidSnapper.LoadEventNexus(
                "Loading Event Nexus for {} ...".format(self.rawDataPath),
                Filename=self.rawDataPath,
                OutputWorkspace=self.inputWStof,
                FilterByTofMin=self.TOFMin,
                FilterByTofMax=self.TOFMax,
                BlockList="Phase*,Speed*,BL*:Chop:*,chopper*TDC",
            )
        # rebin the TOF data logarithmically
        self.convertUnitsAndRebin(self.inputWStof, self.inputWStof, "TOF")
        # also find d-spacing data and rebin logarithmically
        self.convertUnitsAndRebin(self.inputWStof, self.inputWSdsp)

        focusWSname: str = f"_{self.runNumber}_FocGroup"
        # TODO: enable opening different types of grouping files (eg .nxs)
        self.mantidSnapper.LoadDetectorsGroupingFile(
            "Load XML grouping file",
            InputFile=self.groupingFile,
            InputWorkspace=self.inputWStof,
            OutputWorkspace=focusWSname,
        )

        # get handle to group focusing workspace and retrieve all detector IDs
        self.mantidSnapper.executeQueue()
        focusWS = self.mantidSnapper.mtd[focusWSname]
        self.groupIDs: List[int] = [int(x) for x in focusWS.getGroupIDs()]
        self.subgroupWorkspaceIndices: Dict[int, List[int]] = {}
        for groupID in self.groupIDs:
            groupDetectorIDs = [int(x) for x in focusWS.getDetectorIDsOfGroup(groupID)]
            self.subgroupWorkspaceIndices[groupID] = focusWS.getIndicesFromDetectorIDs(groupDetectorIDs)
        self.mantidSnapper.DeleteWorkspace(
            "Delete temp",
            Workspace=focusWSname,
        )
        self.mantidSnapper.executeQueue()

    def initDIFCTable(self):
        """
        Use the instrument definition to create an initial DIFC table
        Because the created DIFC is inside a matrix workspace, it must
        be manually loaded into a table workspace
        """

        # prepare initial diffraction calibration workspace
        self.mantidSnapper.CalculateDIFC(
            "Calculating initial DIFC values",
            InputWorkspace=self.inputWStof,
            OutputWorkspace="_tmp_difc_ws",
        )
        # convert the calibration workspace into a calibration table
        self.mantidSnapper.CreateEmptyTableWorkspace(
            "Creating table to hold DIFC values",
            OutputWorkspace=self.difcWS,
        )
        self.mantidSnapper.executeQueue()
        tmpDifcWS = self.mantidSnapper.mtd["_tmp_difc_ws"]
        DIFCtable = self.mantidSnapper.mtd[self.difcWS]
        DIFCtable.addColumn(type="int", name="detid", plottype=6)
        DIFCtable.addColumn(type="double", name="difc", plottype=6)
        DIFCtable.addColumn(type="double", name="difa", plottype=6)
        DIFCtable.addColumn(type="double", name="tzero", plottype=6)
        DIFCtable.addColumn(type="double", name="tofmin", plottype=6)
        detids = [int(x) for x in tmpDifcWS.extractX()]
        difcs = [float(x) for x in tmpDifcWS.extractY()]
        # TODO why is detid always 1 in tests?
        for detid, difc in zip(detids, difcs):
            DIFCtable.addRow(
                {
                    "detid": detid,
                    "difc": difc,
                    "difa": 0,
                    "tzero": 0,
                    "tofmin": 0,
                }
            )
        self.mantidSnapper.DeleteWorkspace(
            "Delete temp calibration workspace",
            Workspace="_tmp_difc_ws",
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

        rebinParams: str
        if target == "dSpacing":
            rebinParams = f"{self.overallDMin},{-abs(self.dBin)},{self.overallDMax}"
        elif target == "TOF":
            rebinParams = f"{self.TOFMin},{-abs(self.TOFBin)},{self.TOFMax}"

        self.mantidSnapper.Rebin(
            "Rebin the workspace logarithmically",
            InputWorkspace=outputWS,
            OutputWorkspace=outputWS,
            Params=rebinParams,
        )
        self.mantidSnapper.executeQueue()

    # TODO: replace the median with some better method, to be determined
    # for choosing a reference pixel (and not using brightest pixel)
    def getRefID(self, detectorIDs: List[int]) -> int:
        """
        Calculate a unique reference pixel for a pixel grouping, based in the pixel geometry.
        input:
            subgroupIDs: List[int] -- a list of all of the detector IDs in that group
        output:
            the median pixel ID (to be replaced with angular COM pixel)
        """
        return int(np.median(detectorIDs))

    def reexecute(self) -> None:
        """
        Execute the main algorithm, in a way that can be iteratively called.
        First the initial DIFC values must be calculated.  Then, group-by-group,
        the spectra are cross-correlated, the offsets calculated, and the original DIFC
        values are corrected by the offsets.
        outputs:
            data: dict -- several statistics of the offsets, for testing convergence
            OuputWorkspace: str -- the name of the TOF data with new DIFCs applied
            CalibrationTable: str -- the final table of DIFC values
        """
        data: Dict[str, float] = {}
        totalOffsetWS: str = f"offsets_{self.runNumber}"
        wsoff: str = f"_{self.runNumber}_tmp_subgroup_offset"
        wscc: str = f"_{self.runNumber}_tmp_subgroup_CC"

        for groupID, groupWorkspaceIndices in self.subgroupWorkspaceIndices.items():
            refID: int = self.getRefID(groupWorkspaceIndices)
            self.mantidSnapper.CrossCorrelate(
                f"Cross-Correlating spectra for {wscc}",
                InputWorkspace=self.inputWSdsp,
                OutputWorkspace=wscc,
                ReferenceSpectra=refID,
                WorkspaceIndexList=groupWorkspaceIndices,
                XMin=self.overallDMin,
                XMax=self.overallDMax,
                MaxDSpaceShift=self.maxDSpaceShifts,
            )
            self.mantidSnapper.GetDetectorOffsets(
                f"Calculate offset workspace {wsoff}",
                InputWorkspace=wscc,
                OutputWorkspace=wsoff,
                Step=abs(self.dBin),  # Step must be positive
                XMin=-100,
                XMax=100,
                OffsetMode="Signed",
                MaxOffset=2,
            )
            # add in group offsets to total, or begin the sum if none
            if groupID == self.groupIDs[0]:
                self.mantidSnapper.CloneWorkspace(
                    f"Starting summation with offset workspace {wsoff}",
                    InputWorkspace=wsoff,
                    OutputWorkspace=totalOffsetWS,
                )
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
        data["medianOffset"] = abs(np.median(offsets))
        data["meanOffset"] = abs(np.mean(offsets))
        data["minOffset"] = abs(np.min(offsets))
        data["maxOffset"] = abs(np.max(offsets))

        # get difcal corrected by offsets
        calibrationWS = f"_{self.runNumber}_CAL_CC_temp"
        # TODO: replace this with appropriately edited mantid version
        self.mantidSnapper.ConvertDiffCalLog(
            "Correct previous calibration constants by offsets",
            OffsetsWorkspace=totalOffsetWS,
            PreviousCalibration=self.difcWS,
            OutputWorkspace=calibrationWS,
            BinWidth=self.dBin,
        )

        # save the resulting DIFC for starting point in next iteration
        self.mantidSnapper.RenameWorkspace(
            "Save the DIFC for starting point in next iteration",
            InputWorkspace=calibrationWS,
            OutputWorkspace=self.difcWS,
            OverwriteExisting=True,
        )

        # apply offset correction to input workspace
        self.mantidSnapper.ApplyDiffCal(
            "Apply the diffraction calibration to the input TOF workspace",
            InstrumentWorkspace=self.inputWStof,
            CalibrationWorkspace=self.difcWS,
        )
        # convert to d-spacing and rebin logarithmically
        self.convertUnitsAndRebin(self.inputWStof, self.inputWSdsp)

        # cleanup memory usage
        self.mantidSnapper.DeleteWorkspace(
            f"Deleting temporary cross-correlation workspace {wscc}",
            Workspace=wscc,
        )
        self.mantidSnapper.DeleteWorkspace(
            f"Deleting temporary offset workspace {wsoff}",
            Workspace=wsoff,
        )
        self.mantidSnapper.DeleteWorkspace(
            "Deleting total offset workspace",
            Workspace=totalOffsetWS,
        )
        # now execute the queue
        self.mantidSnapper.executeQueue()
        # data["calibrationTable"] = self.difcWS
        self.setProperty("data", json.dumps(data))
        self.setProperty("OutputWorkspace", self.inputWStof)
        self.setProperty("CalibrationTable", self.difcWS)

    def PyExec(self) -> None:
        """
        Calculate pixel calibration DIFC values on each spectrum group.
        inputs:
            Ingredients: DiffractionCalibrationIngredients -- the DAO holding data needed to run the algorithm
        outputs:
            data: dict -- several statistics of the offsets, for testing convergence
            OuputWorkspace: str -- the name of the TOF data with new DIFCs applied
            CalibrationTable: str -- the final table of DIFC values
        """
        self.log().notice("Extraction of calibration constants START!")

        # get the ingredients
        ingredients = DiffractionCalibrationIngredients(**json.loads(self.getProperty("Ingredients").value))
        self.chopIngredients(ingredients)

        # load and process the input data for algorithm
        self.retrieveFromPantry()
        self.initDIFCTable()

        # now calculate and correct by offsets
        self.reexecute()


# Register algorithm with Mantid
AlgorithmFactory.subscribe(CalculateOffsetDIFC)
