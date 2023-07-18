import json

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
        self.declareProperty(
            "DiffractionCalibrationIngredients", defaultValue="", direction=Direction.Input
        )  # noqa: F821
        self.declareProperty("CalibrationWorkspace", defaultValue="", direction=Direction.Output)
        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, name)

    # TODO: ensure all ingredients loaded elsewhere, no superfluous ingredients
    def chopIngredients(self, ingredients):
        """Receive the ingredients from the recipe, and exctract the needed pieces for this algorithm."""
        self.runNumber = ingredients.runConfig.runNumber
        self.ipts = ingredients.runConfig.IPTS
        self.rawDataPath = self.ipts + "shared/lite/SNAP_{}.lite.nxs.h5".format(ingredients.runConfig.runNumber)

        # TODO: cleanup unneeded input parameters
        self.isLite = True
        self.stateFolder = "/SNS/SNAP/shared/Calibration/Powder/04bd2c53f6bf6754/"

        # fdrom the instrument state, read the overall min/max TOF
        self.instrumentState = ingredients.instrumentState
        self.TOFMin = self.instrumentState.particleBounds.tof.minimum
        self.TOFMax = self.instrumentState.particleBounds.tof.maximum
        self.TOFBin = -0.001

        # from grouping parameters, read the overall min/max d-spacings
        self.focusGroup = ingredients.focusGroup
        self.overallDMin = max(self.focusGroup.dMin)
        self.overallDMax = min(self.focusGroup.dMax)
        self.dBin = -min([abs(x) for x in self.focusGroup.dBin])  # ensure dBin is negative for log binning
        self.maxDSpaceShifts = 2.5 * max(self.focusGroup.FWHM)

        # path to grouping file, specifying group IDs of pixels
        # TODO: allow for other grouping files, such as for SNAPLite
        # TODO: allow for other grouping schemes (eg Bank, etc.)
        self.groupingFile = "/SNS/SNAP/shared/Calibration/Powder/PixelGroupingDefinitions/SNAPFocGroup_Column.xml"

        # focused grouping ws
        self.focusWSname = f"_{self.runNumber}_FocGroup"

    def initializeRawDataWorkspace(self):
        """Initialize the input TOF data from the input filename in the ingredients"""
        self.mantidSnapper.LoadEventNexus(
            "Loading Event Nexus for {} ...".format(self.rawDataPath),
            Filename=self.rawDataPath,
            FilterByTofMin=2000,  # TODO: why these values?
            FilterByTofMax=14500,
            OutputWorkspace=self.inputWStof,
        )

        # rebin the TOF data logarithmically
        self.convertUnitsAndRebin(self.inputWStof, self.inputWStof, "TOF")
        # also find d-spacing data and rebin logarithmically
        self.convertUnitsAndRebin(self.inputWStof, self.inputWSdsp)

        # TODO: enable opening different types of grouping files (eg .nxs)
        self.mantidSnapper.LoadDetectorsGroupingFile(
            "Load XML grouping file",
            InputFile=self.groupingFile,
            OutputWorkspace=self.focusWSname,
        )

        # get handle to group focusing workspace
        self.mantidSnapper.executeQueue()
        self.focusWS = self.mantidSnapper.mtd[self.focusWSname]
        self.groupIDs = self.focusWS.getGroupIDs()

    def convertUnitsAndRebin(self, inputWS, outputWS, target="dSpacing"):
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

        rebinParams = ""
        if target == "dSpacing":
            rebinParams = f"{self.overallDMin},{-abs(self.dBin)},{self.overallDMax}"
        elif target == "TOF":
            rebinParams = f"{self.TOFMin},{-abs(self.TOFBin)},{self.TOFMax}"

        self.mantidSnapper.Rebin(
            "Rebin the workspace logarithmically",
            InputWorkspace=outputWS,
            Params=rebinParams,
            OutputWorkspace=outputWS,
        )
        self.mantidSnapper.executeQueue()

    # TODO: replace the median with some better method, to be determined
    # for choosing a reference pixel (and not using brightest pixel)
    def getRefID(self, subgroupIDs):
        """
        Calculate a unique reference pixel for a pixel grouping, based in the pixel geometry.
        input:
            subgroupIDs: List[int] -- a list of all of the detector IDs in that group
        output:
            the median pixel ID (to be replaced with angular COM pixel)
        """
        return int(np.median(subgroupIDs))

    def reexecute(self, difcWS):
        """
        Execute the main algorithm, in a way that can be iteratively called.
        First the initial DIFC values must be calculated.  Then, group-by-group,
        the spectra are cross-correlated, the offsets calculated, and the original DIFC
        values are corrected by the offsets.
        input:
            difcWS: str -- the name of workspace holding the calibration constants, DIFC
        output:
            data: dict -- several statistics of the offsets, for testing convergence
        """
        data = {}
        totalOffsetWS = f"offsets_{self.runNumber}"
        wsoff = f"_{self.runNumber}_tmp_subgroup_offset"
        wscc = f"_{self.runNumber}_tmp_subgroup_CC"
        for subGroup in self.groupIDs:
            subGroupIDs = self.focusWS.getDetectorIDsOfGroup(int(subGroup))
            maxDspaceShift = self.maxDSpaceShifts
            refID = self.getRefID(subGroupIDs)

            self.mantidSnapper.CrossCorrelate(
                f"Cross-Correlating spectra for {wscc}",
                InputWorkspace=self.inputWSdsp,
                OutputWorkspace=wscc,
                ReferenceSpectra=refID,
                WorkspaceIndexList=subGroupIDs,
                XMin=self.overallDMin,
                XMax=self.overallDMax,
                MaxDSpaceShift=maxDspaceShift,
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
            if subGroup == self.groupIDs[0]:
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
        # TODO: replace this with ConvertDiffCal edited to work in log-space
        self.mantidSnapper.ConvertDiffCalLog(
            "Correct previous calibration constants by offsets",
            OffsetsWorkspace=totalOffsetWS,
            PreviousCalibration=difcWS,
            OutputWorkspace=calibrationWS,
            BinWidth=self.dBin,
        )

        # save the resulting DIFC for starting point in next iteration
        # TODO: this might not be necessary when ConverDiffCal is edited
        self.mantidSnapper.ConvertTableToMatrixWorkspace(
            "Save the DIFC for starting point in next iteration",
            InputWorkspace=calibrationWS,
            OutputWorkspace=difcWS,
            ColumnX="detid",
            ColumnY="difc",
        )

        # apply offset correction to input workspace
        self.mantidSnapper.ApplyDiffCal(
            "Apply the diffraction calibration to the input TOF workspace",
            InstrumentWorkspace=self.inputWStof,
            CalibrationWorkspace=calibrationWS,
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
            "Deleting used calibration workspace",
            Workspace=calibrationWS,
        )
        self.mantidSnapper.DeleteWorkspace(
            "Deleting total offset workspace",
            Workspace=totalOffsetWS,
        )
        # now execute the queue
        self.mantidSnapper.executeQueue()
        return data

    def PyExec(self):
        """Run the algo, including processing ingredients and initializing the input form file"""
        self.log().notice("Execution of extraction of calibration constants START!")

        # get the ingredients
        ingredients = DiffractionCalibrationIngredients(
            **json.loads(self.getProperty("DiffractionCalibrationIngredients").value)
        )
        self.chopIngredients(ingredients)

        # create string names of workspaces that will be used by algorithm
        self.inputWSdsp = f"_DSP_{self.runNumber}_raw"
        self.inputWStof = f"_TOF_{self.runNumber}_raw"
        self.difcWS = self.getProperty("CalibrationWorkspace").value

        # load data from file
        self.initializeRawDataWorkspace()

        # prepare initial diffraction calibration table
        self.mantidSnapper.CalculateDIFC(
            "Calculating initial DIFC values",
            InputWorkspace=self.inputWStof,
            OutputWorkspace=self.difcWS,
        )

        # now calculate and correct by offsets
        return self.reexecute(self.difcWS)


# Register algorithm with Mantid
AlgorithmFactory.subscribe(CalculateOffsetDIFC)
