# @file: tests/util/diffraction_calibration_synthetic_data.py:
#
# Preparation of synthetic data for the following unit-test classes:
#   * TestPixelDiffractionCalibration
#   * TestGroupDiffractionCalibration
#   * TestDiffractionCalibrationRecipe
#

import secrets
from collections import namedtuple
from pathlib import Path
from typing import Dict, List, Tuple

import mantid
import numpy as np
from mantid.simpleapi import (
    ConvertUnits,
    CreateSampleWorkspace,
    LoadDetectorsGroupingFile,
    LoadInstrument,
    Rebin,
    RebinRagged,
    ScaleX,
    mtd,
)
from snapred.backend.dao import CrystallographicInfo
from snapred.backend.dao.DetectorPeak import DetectorPeak
from snapred.backend.dao.GroupPeakList import GroupPeakList
from snapred.backend.dao.ingredients import DiffractionCalibrationIngredients
from snapred.backend.dao.RunConfig import RunConfig
from snapred.meta.Config import Resource
from util.dao import DAOFactory
from util.helpers import *

Peak = namedtuple("Peak", "centre sigma height")


def random_seed(bits: int) -> int:
    # Generate a random seed of 'bits' length.
    return secrets.randbits(bits)


class SyntheticData(object):
    """_Fixed_ implementation of synthetic data to be used for all diffraction-calibration tests."""

    RANDOM_SEED = 311379324803478887040360489933500222827

    # MOCK instruments and configuration files:
    fakeInstrumentFilePath = Resource.getPath("inputs/testInstrument/fakeSNAP_Definition.xml")
    fakeGroupingFilePath = Resource.getPath("inputs/testInstrument/fakeSNAPFocGroup_Natural.xml")

    # REAL instruments and configuration files:
    SNAPInstrumentFilePath = str(Path(mantid.__file__).parent / "instrument" / "SNAP_Definition.xml")
    SNAPLiteInstrumentFilePath = Resource.getPath("inputs/pixel_grouping/SNAPLite_Definition.xml")

    def __init__(self, workspaceType: str = "Histogram", scale: float = 1000.0):
        fakeRunNumber = "555"
        self.fakeRunConfig = RunConfig(
            runNumber=str(fakeRunNumber),
            IPTS="",
        )

        self.fakeInstrumentState = DAOFactory.synthetic_instrument_state
        self.fakeFocusGroup = DAOFactory.synthetic_focus_group_natural
        self.fakePixelGroup = DAOFactory.synthetic_pixel_group

        # Place all peaks within the _minimum_ d-space range of any pixel group.
        dMin = max(self.fakePixelGroup.dMin())
        dMax = min(self.fakePixelGroup.dMax())

        # Overall _magnitude_ scale factor:
        self.scale = scale

        self.workspaceType = workspaceType

        # The pixel group's TOF-domain will be used to convert the original `CreateSampleWorkspace` 'Powder Diffraction'
        #   predefined function: this allows peak widths to be properly scaled to generate data for a d-spacing domain.
        TOFMin = self.fakePixelGroup.timeOfFlight.minimum
        TOFMax = self.fakePixelGroup.timeOfFlight.maximum
        self.peaks, self.background_function = SyntheticData._fakePowderDiffractionPeakList(
            TOFMin, TOFMax, dMin, dMax, self.scale
        )

        crystalPeaks = SyntheticData.crystalInfo().peaks

        peakList = [
            DetectorPeak.model_validate(
                {
                    "position": {
                        "value": p.centre,
                        "minimum": p.centre - SyntheticData.fwhmFromSigma(p.sigma) / 2.0,
                        "maximum": p.centre + SyntheticData.fwhmFromSigma(p.sigma) / 2.0,
                    },
                    "peak": crystalPeaks[i].dict(),
                }
            )
            for i, p in enumerate(self.peaks)
        ]

        # For testing purposes: every pixel group will use the same peak list;
        #   in spite of the offset shifts applied previously: these are still the correct _target_ peak locations.
        peakLists: Dict[int, List[DetectorPeak]] = {2: peakList, 3: peakList, 7: peakList, 11: peakList}
        maxFWHM = SyntheticData.fwhmFromSigma(max([p.sigma for p in self.peaks]))

        self.ingredients = DiffractionCalibrationIngredients(
            runConfig=self.fakeRunConfig,
            groupedPeakLists=[
                GroupPeakList(groupID=key, peaks=peakLists[key], maxfwhm=maxFWHM) for key in peakLists.keys()
            ],
            convergenceThreshold=0.5,
            maxOffset=100.0,  # bins: '100.0' seems to work
            pixelGroup=self.fakePixelGroup,
            maxChiSq=100.0,
            skipPixelCalibration=False,
        )

    @staticmethod
    def fakeDetectorPeaks(scale: float = 1000.0) -> List[DetectorPeak]:
        fakePixelGroup = DAOFactory.synthetic_pixel_group.copy()

        # Place all peaks within the _minimum_ d-space range of any pixel group.
        dMin = min(fakePixelGroup.dMin())
        dMax = max(fakePixelGroup.dMax())

        # The pixel group's TOF-domain will be used to convert the original `CreateSampleWorkspace` 'Powder Diffraction'
        #   predefined function: this allows peak widths to be properly scaled to generate data for a d-spacing domain.
        TOFMin = fakePixelGroup.timeOfFlight.minimum
        TOFMax = fakePixelGroup.timeOfFlight.maximum
        peaks, _ = SyntheticData._fakePowderDiffractionPeakList(TOFMin, TOFMax, dMin, dMax, scale)

        crystalPeaks = SyntheticData.crystalInfo().peaks

        peakList = [
            DetectorPeak.model_validate(
                {
                    "position": {
                        "value": p.centre,
                        "minimum": p.centre - SyntheticData.fwhmFromSigma(p.sigma) / 2.0,
                        "maximum": p.centre + SyntheticData.fwhmFromSigma(p.sigma) / 2.0,
                    },
                    "peak": crystalPeaks[i].dict(),
                }
            )
            for i, p in enumerate(peaks)
        ]
        return peakList

    @staticmethod
    def random_seed(bits: int) -> int:
        # Generate a random seed of 'bits' length.
        return secrets.randbits(bits)

    @staticmethod
    def fwhmFromSigma(sigma: float) -> float:
        return 2.0 * np.sqrt(2.0 * np.log(2.0)) * sigma

    @staticmethod
    def _fakePowderDiffractionPeakList(
        tofMin: float, tofMax: float, dMin: float, dMax: float, scale: float
    ) -> Tuple[List[Tuple[float, float, float]], str]:
        """Duplicate the `CreateSampleWorkspace` 'Powder Diffraction' predefined function,
             but in d-spacing instead of TOF units.
        -- returns (List[Peak(centre, sigma, height)], <background function string>)
        """
        # 'mantid/Framework/Algorithms/src/CreateSampleWorkspace.cpp': lines 85-94:
        """
          m_preDefinedFunctionmap.emplace("Powder Diffraction",
                                          "name= LinearBackground,A0=0.0850208,A1=-4.89583e-06;"
                                          "name=Gaussian,Height=0.584528,PeakCentre=$PC1$,Sigma=14.3772;"
                                          "name=Gaussian,Height=1.33361,PeakCentre=$PC2$,Sigma=15.2516;"
                                          "name=Gaussian,Height=1.74691,PeakCentre=$PC3$,Sigma=15.8395;"
                                          "name=Gaussian,Height=0.950388,PeakCentre=$PC4$,Sigma=19.8408;"
                                          "name=Gaussian,Height=1.92185,PeakCentre=$PC5$,Sigma=18.0844;"
                                          "name=Gaussian,Height=3.64069,PeakCentre=$PC6$,Sigma=19.2404;"
                                          "name=Gaussian,Height=2.8998,PeakCentre=$PC7$,Sigma=21.1127;"
                                          "name=Gaussian,Height=2.05237,PeakCentre=$PC8$,Sigma=21.9932;"
                                          "name=Gaussian,Height=8.40976,PeakCentre=$PC9$,Sigma=25.2751;");
        """
        # Due to (what is felt to be) some type of rebinning issue in `PDCalibration`:
        #   it was found necessary to apply an additional magnitude scale factor to the generated data.
        # TODO: track down the reason for this and create an associated DEFECT.

        centres = np.arange(dMin, dMax, 0.1 * (dMax - dMin))
        dspDomain = dMax - dMin
        tofDomain = tofMax - tofMin
        peaks = [
            Peak(centres[1], 14.3772 * dspDomain / tofDomain, scale * 0.584528),
            Peak(centres[2], 15.2516 * dspDomain / tofDomain, scale * 1.33361),
            Peak(centres[3], 15.8395 * dspDomain / tofDomain, scale * 1.74691),
            Peak(centres[4], 19.8408 * dspDomain / tofDomain, scale * 0.950388),
            Peak(centres[5], 18.0844 * dspDomain / tofDomain, scale * 1.92185),
            Peak(centres[6], 19.2404 * dspDomain / tofDomain, scale * 3.64069),
            Peak(centres[7], 21.1127 * dspDomain / tofDomain, scale * 2.8998),
            Peak(centres[8], 21.9932 * dspDomain / tofDomain, scale * 2.05237),
            Peak(centres[9], 25.2751 * dspDomain / tofDomain, scale * 8.40976),
        ]
        background = f"name= LinearBackground,A0={scale * 0.0850208},A1={scale * -4.89583e-06 * dspDomain / tofDomain};"
        return peaks, background

    def generateWorkspaces(self, rawWS: str, groupingWS: str, maskWS: str) -> None:
        """Generate initialized workspace data:
        -- rawWS: an input workspace in TOF units;
        -- groupingWS: the associated grouping workspace;
        -- maskWS: a compatible mask workspace
        """
        # IMPORTANT:
        #   Q: Why does this `generateWorkspaces` method exist as a public method,
        #        rather than just immediately generating these workspaces in the '__init__' and saving them?
        #   A: In order to avoid collisions in the Analysis Data Service during _parallel_ test runs,
        #        especially when input workspaces are modified: unique workspace names must be used.
        #      This means that best practice would be to either generate (or clone) workspace data at point-of-use
        #        for each test method.
        #      Then, either a per-test setup method can be used (assuming unique names are used),
        #        or a per-class setup method can be used, with cloning to new workspaces with unique names.

        # Notes:
        #   * The design idea behind this data initialization is to start with _perfectly_ calibrated data,
        #       and then work backwards.
        #     In d-spacing, spectra from such data will have zero relative offsets (between pixels).
        #   * A random normal distribution (with a known seed) is used to modify the starting data,
        #       with the offset shifts limited to physically-meaningful values.
        #   * The actual modification used corresponds precisely to the d-space to TOF scaling (using DIFC).
        #       This is more physically relevant than just shifting the data by the offset-shifts.
        #       (However, as random offset shifts are used, this is probably a fine point.)
        #   * Note that for many of the diffraction-calibration processing steps, units are in BINs,
        #       and not in d-spacing or TOF.
        #   * Diffraction-calibration methods expect logarithmically binned input data in TOF units;
        #   * Mathematically: since TOF is largely a scale-factor (DIFC) conversion from d-spacing,
        #       properly logarithmically-binned d-spacing data, converted to TOF,
        #       in many cases will automatically have the proper TOF binning; However, this will _only_ be true
        #       if the TOF-domain itself has been properly converted from the d-spacing domain.
        #   * It's quite important that any rebinning use the fact that for converted data,
        #       the appropriate bin size for logarithmic binning will be _exactly_ the _same_
        #       for both d-spacing and converted-to-TOF units.  If this requirement is not satisfied _rigorously_,
        #       the following rebinning artifacts may be induced, to the specifics of the rebinning process:
        #         - artificial piecewise-linear smoothing (or interpolation) of the data;
        #         - rebinning aliasing (and associated oscillation) in the calibration convergence loop;
        #         - data rescaling issues: i.e. data magnitudes are much smaller (or larger) than expected.

        dMin = self.ingredients.pixelGroup.dMin()
        dMax = self.ingredients.pixelGroup.dMax()
        dBin = self.ingredients.pixelGroup.dBin()
        overallDMin = min(dMin)
        overallDMax = max(dMax)

        dBin_abs = max([abs(d) for d in dBin])

        functionString = self.background_function
        for p in self.peaks:
            functionString += f"name=Gaussian, PeakCentre={p.centre}, Height={p.height}, Sigma={p.sigma};"

        # Create the input workspace 'rawWS':
        CreateSampleWorkspace(
            OutputWorkspace=rawWS,
            WorkspaceType=self.workspaceType,
            Function="User Defined",
            UserDefinedFunction=functionString,
            Xmin=overallDMin,
            Xmax=overallDMax,
            BinWidth=dBin_abs,
            XUnit="dSpacing",
            NumBanks=4,  # must produce same number of pixels as fake instrument
            BankPixelWidth=2,  # each bank has 4 pixels, 4 banks, 16 total
        )

        LoadInstrument(
            Workspace=rawWS,
            Filename=self.fakeInstrumentFilePath,
            RewriteSpectraMap=True,
        )

        # Load the grouping workspace 'groupingWS':
        LoadDetectorsGroupingFile(
            InputFile=self.fakeGroupingFilePath,
            InputWorkspace=rawWS,
            OutputWorkspace=groupingWS,
        )

        focWS = mtd[groupingWS]
        detectors = focWS.detectorInfo()
        allXmins = [0] * 16
        allXmaxs = [0] * 16
        allDelta = [0] * 16
        for i, gid in enumerate(focWS.getGroupIDs()):
            for detid in focWS.getDetectorIDsOfGroup(int(gid)):
                det_index = detectors.indexOf(int(detid))
                allXmins[det_index] = dMin[i]
                allXmaxs[det_index] = dMax[i]
                allDelta[det_index] = dBin[i]
        RebinRagged(
            InputWorkspace=rawWS,
            OutputWorkspace=rawWS,
            XMin=allXmins,
            XMax=allXmaxs,
            Delta=allDelta,
        )

        # Shift the spectra so that they have differing offsets
        _ws = mtd[rawWS]
        shiftSigma = 10.0  # bins
        binOffsetShifts = np.random.default_rng(SyntheticData.RANDOM_SEED).normal(
            loc=0.0, scale=shiftSigma, size=_ws.getNumberHistograms()
        )
        for ns in range(_ws.getNumberHistograms()):
            bin_width = abs(allDelta[ns])
            offset = binOffsetShifts[ns] * (dBin_abs / bin_width)  # retain normal distribution
            factor = np.power(1.0 + bin_width, offset)
            ScaleX(
                InputWorkspace=rawWS,
                OutputWorkspace=rawWS,
                IndexMin=ns,
                IndexMax=ns,
                Factor=factor,
                Operation="Multiply",
            )

        # Convert to non-ragged workspace:
        Rebin(
            InputWorkspace=rawWS,
            OutputWorkspace=rawWS,
            Params=(overallDMin, dBin_abs, overallDMax),
            BinningMode="Logarithmic",
        )

        # Convert to TOF units (binning is already correct):
        ConvertUnits(
            InputWorkspace=rawWS,
            OutputWorkspace=rawWS,
            Target="TOF",
        )

        # Create the mask workspace 'maskWS':
        createCompatibleMask(maskWS, rawWS)

    def crystalInfo():
        return CrystallographicInfo.model_validate_json(Resource.read("outputs/crystalinfo/output.json"))
