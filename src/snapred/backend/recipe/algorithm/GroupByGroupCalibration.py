from mantid.api import AlgorithmFactory, PythonAlgorithm
from mantid.kernel import Direction

from snapred.backend.dao.DiffractionCalibrationIngredients import DiffractionCalibrationIngredients
from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper

name = "GroupByGroupCalibration"


class GroupByGroupCalibration(PythonAlgorithm):
    def PyInit(self):
        # declare properties
        self.declareProperty("Ingredients", defaultValue="", direction=Direction.Input)  # noqa: F821
        self.declareProperty("OutputWorkspace", defaultValue="", direction=Direction.Output)
        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, name)

    def chopIngredients(self, ingredients):
        self.runNumber = ingredients.runConfig.runNumber
        self.gPrm = ingredients.focusGroup  # see the FocusGroup DAO and PixelGroupingParameters DAO
        self.sPrm = ingredients.instrumentState  # see the InstrumentState DAO

        self.TOFMin = self.sPrm["particleBounds"]["tof"]["minimum"]
        self.TOFMax = self.sPrm["particleBounds"]["tof"]["maximum"]

    def PyExec(self):
        # run the algo
        self.log().notice("Execution of extraction of calibration constants START!")

        # get the ingredients
        ingredients = DiffractionCalibrationIngredients(**json.loads(self.getProperty("Ingredients").value))
        self.chopIngredients(ingredients)

        # input workspaces will be raw TOF data after offset-corrections
        recalibratedWSdsp = inputWSdsp
        # the workspaces after diffraction focusing
        diffractionfocusedWSdsp = f"_DSP_{self.runNumber}_cal_CC_"
        diffractionfocusedWStof = f"_TOF_{self.runNumber}_cal_CC_"

        self.mantidSnapper.DiffractionFocussing(
            "Refocus with offset-corrections",
            InputWorkspace=recalibratedWSdsp,
            GroupingWorkspace=focusWSname,
            OutputWorkspace=diffractionfocusedWSdsp,
        )
        self.mantidSnapper.ConvertUnits(
            "Convert from TOF to d-spacing",
            InputWorkspace=diffractionfocusedWSdsp,
            OutputWorkspace=diffractionfocusedWStof,
            Target="TOF",
        )

        # PDCalibration executed group by group, so need handle on focused ws
        ws_foc = self.manidSnapper.mtd[diffractionfocusedWStof]

        # Before running PDCalibration get original DIFC values for group
        self.mantidSnapper.CreateDetectorTable(
            "Prepare detector table",
            InputWorkspace=diffractionfocusedWStof,
            DetectorTableWorkspace=f"_DetTable_{self.runNumber}_cal_CC_CC",
        )

        # Create new cal workspace that will be modified after PDCalibration
        # create diffCal workspace with new values.
        CAL_CC_PD = self.mantidSnapper.CreateEmptyTableWorkspace(
            "Prepare table for offset and calibration corrected DIFC",
        )
        CAL_CC_PD.addColumn(type="int", name="detid", plottype=6)
        CAL_CC_PD.addColumn(type="float", name="difc", plottype=6)
        CAL_CC_PD.addColumn(type="float", name="difa", plottype=6)
        CAL_CC_PD.addColumn(type="float", name="tzero", plottype=6)
        CAL_CC_PD.addColumn(type="float", name="tofmin", plottype=6)
        CAL_CC_PD.setLinkedYCol(0, 1)

        # calibrationWS is the result of the CalculateDiffractionOffsers algorithm
        origCal = mtd[calibrationWS].toDict()

        for subGroup in GroupIDs:
            subGroupIDs = focusWS.getDetectorIDsOfGroup(int(subGroup))
            len(subGroupIDs)
            max(calibPeakListSorted) * self.gPrm["delDOverD"][int(subGroup) - 1] * 2.35

            # load get d-spacings, apply group d-limits and purge weak reflections
            # this is from SNAPTools (see prototype repo)
            # should be ewuivalent to IngestCrystallographicInfoAlgorithm
            calibPeakList = snp.peakPosFromCif(
                cifPath=calibrantCif,
                Ithresh=0.01,
                dMin=gPrm["dMin"][subGroup - 1],
                dMax=gPrm["dMax"][subGroup - 1],
                verbose=False,
            )
            np.array(sorted(calibPeakList, key=float))

            # getDIFC
            ws_foc.spectrumInfo().difcUncalibrated(int(subGroup - 1))

            # extract spectrum to fitfunctions
            self.mantidSnapper.ExtractSpectra(
                f"Remove the spectra from subgroup {subGroup}",
                InputWorkspace=diffractionfocusedWStof,
                # XMin = gPrm["dMin"][subGroup-1],
                # XMax = gPrm["dMax"][subGroup-1],
                StartWorkspaceIndex=int(subGroup) - 1,
                EndWorkspaceIndex=int(subGroup) - 1,
                outputWorkspace=f"_TOF_{run}_cal_CC_subGroup{subGroup}",
            )

            # this is from SNAPTools (see prototype repo)
            # should be ewuivalent to PurgeOverlapperPeaks
            peaks, peakBoundaries = snp.removeOverlappingPeaks(
                calibrantCif, self.sPrm, self.gPrm, subGroup  # calibrantCif is a path to a CIF file
            )

            # run PDCalibration on extracted ws
            self.mantidSnapper.PDCalibration(
                f"Perform PDCalibration on subgroup {subGroup}",
                InputWorkspace=f"_TOF_{self.runNumber}_cal_CC_subGroup{subGroup}",
                TofBinning=f"{self.TOFMin},{self.TOFBin},{self.TOFMax}",
                PeakFunction="Gaussian",
                BackgroundType="Linear",
                PeakPositions=peaks,
                PeakWindow=peakBoundaries,
                CalibrationParameters="DIFC",
                HighBackground=True,
                OutputCalibrationTable=f"_PDCal_table_{subGroup}",
                DiagnosticWorkspaces=f"_PDCal_diag_{subGroup}",
            )

            # OutputCalibrationTable contrains values for full instrument, but only those with
            # pixels in the group have the corrected DIFC.

            # this is the same operation performed in CombineDifCal, but on group-by-group
            # might be easier way to do this

            table = self.mantidSnapper.mtd[f"_PDCal_table_{subGroup}"].toDict()
            DIFC_PD = table["difc"][subGroupIDs[0]]  # take from first pixel in group

            table = self.mantidSnapper.mtd[f"_DetTable_{self.runNumber}_cal_CC_Column_CC"].toDict()
            DIFC_CC = table["DIFC"][int(subGroup) - 1]

            # correction factor is ratio of group CC DIFC and group PD DIFC
            PDFactor = DIFC_PD / DIFC_CC

            for i in subGroupIDs:
                nextRow = {"detid": i, "difc": origCal["difc"][i] * PDFactor, "difa": 0, "tzero": 0, "tofmin": 0}
                CAL_CC_PD.addRow(nextRow)

            # apply calibration to inspect results

            # this step can almost certainly be removed
            self.mantidSnapper.CloneWorkspace(
                "Copy the input", InputWorkspace=rawInputWS, OutputWorkspace=f"_TOF_{self.runNumber}_cal_CC_PD"
            )

            # now apply the final corrected DIFCs to measurements
            self.mantidSnapper.ApplyDiffCal(
                "Applying resulting calibration to data",
                InstrumentWorkspace=f"_TOF_{self.runNumber}_cal_CC_PD",
                CalibrationWorkspace="CAL_CC_PD",
            )
            ConvertUnits(
                "Calculating results in d-spacing",
                Inputworkspace=f"_TOF_{self.runNumber}_cal_CC_PD",
                OutputWorkspace=f"_DSP_{self.runNumber}_cal_CC_PD",
                Target="dSpacing",
            )
            DiffractionFocussing(
                "Diffraction focussing result",
                InputWorkspace=f"_DSP_{run}_cal_CC_PD",
                GroupingWorkspace=focusWSname,
                OutputWorkspace=f"_DSP_{run}_cal_CC_PD_",
            )

    # after this, the recipe needs to save the resulting workspace


# Register algorithm with Mantid
AlgorithmFactory.subscribe(GroupByGroupCalibration)
