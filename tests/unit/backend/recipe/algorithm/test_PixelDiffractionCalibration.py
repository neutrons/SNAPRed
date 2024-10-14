import unittest
from collections.abc import Sequence
from itertools import permutations
from unittest import mock

from mantid.api import MatrixWorkspace
from mantid.simpleapi import mtd

# needed to make mocked ingredients
# the algorithm to test
from snapred.backend.recipe.algorithm.PixelDiffractionCalibration import PixelDiffCalRecipe as Recipe
from snapred.meta.Config import Config
from util.diffraction_calibration_synthetic_data import SyntheticData
from util.helpers import maskSpectra, setSpectraToZero

"""
NOTE this is in fact a test of a recipe.  Its location and name are a
TEMPORARY assignment as part of a refactor.  This helps the git diff
be as useful as possible to reviewing devs.
As soon as the change with this string is merged, this file can be
renamed to `test_PixelDiffCalReipe.py` and moved to the recipe tests folder
"""


class TestPixelDiffractionCalibration(unittest.TestCase):
    def setUp(self):
        """Create a set of mocked ingredients for calculating DIFC corrected by offsets"""
        inputs = SyntheticData()
        self.ingredients = inputs.ingredients

        runNumber = self.ingredients.runConfig.runNumber
        fakeRawData = f"_test_pixelcal_{runNumber}"
        fakeGroupingWorkspace = f"_test_pixelcal_difc_{runNumber}"
        fakeMaskWorkspace = f"_test_pixelcal_difc_{runNumber}_mask"
        inputs.generateWorkspaces(fakeRawData, fakeGroupingWorkspace, fakeMaskWorkspace)
        self.groceries = {
            "inputWorkspace": fakeRawData,
            "maskWorkspace": fakeMaskWorkspace,
            "groupingWorkspace": fakeGroupingWorkspace,
            "calibrationTable": mtd.unique_name(5, "pxdiffcal_"),
        }

    def tearDown(self) -> None:
        # At present tests are not run in parallel, so cleanup the ADS:
        mtd.clear()
        assert len(mtd.getObjectNames()) == 0
        return super().tearDown()

    def test_chop_ingredients(self):
        """Test that ingredients for algo are properly processed"""
        rx = Recipe()
        rx.chopIngredients(self.ingredients)
        assert rx.runNumber == self.ingredients.runConfig.runNumber
        assert rx.overallDMin == min(self.ingredients.pixelGroup.dMin())
        assert rx.overallDMax == max(self.ingredients.pixelGroup.dMax())
        assert rx.dBin == max([abs(db) for db in self.ingredients.pixelGroup.dBin()])

    def test_execute(self):
        """Test that the algorithm executes"""
        rx = Recipe()
        rx.prep(self.ingredients, self.groceries)
        assert rx.execute()

        x = rx.medianOffsets[-1]
        assert x is not None
        assert x != 0.0
        assert x > 0.0
        assert x <= self.ingredients.maxOffset

    def test_reexecution_and_convergence(self):
        """Test that the algorithm can run, and that it will converge to an answer"""
        rx = Recipe()
        result = rx.cook(self.ingredients, self.groceries).medianOffsets

        # check that value converges
        # WARNING: testing for three iterations seems to be about the limit here.
        #   At greater than 3 iterations, there are small oscillations about a limit value.
        maxIter = 3
        assert len(result) < maxIter
        allOffsets = [result[0]]

        # The following assertion will fail if the convergence behavior is oversimplified:
        #   an initial fast convergence (two iterations or so) followed by minor oscillations
        #   should still be considered to be a passing result.
        for i in range(1, len(result)):
            allOffsets.append(result[i])
            assert allOffsets[-1] <= max(1.0e-4, allOffsets[-2])
        assert allOffsets[-1] <= self.ingredients.convergenceThreshold

    def test_execute_ordered(self):
        # produce 4, 2, 1, 0.5
        rx = Recipe()
        rx.mantidSnapper = mock.Mock()
        rx.mantidSnapper.GroupedDetectorIDs.return_value = {}
        rx.mantidSnapper.OffsetStatistics.side_effect = [{"medianOffset": 4 * 2 ** (-i)} for i in range(10)]
        result = rx.cook(self.ingredients, self.groceries)
        assert result.result
        assert result.medianOffsets == [4, 2, 1, 0.5]

    def test_ensure_monotonic(self):
        """
        If the median offsets do not converge monotonically, the recipe stops
        """
        rx = Recipe()
        rx.mantidSnapper = mock.Mock()
        rx.mantidSnapper.GroupedDetectorIDs.return_value = {}
        rx.mantidSnapper.OffsetStatistics.side_effect = [{"medianOffset": x} for x in [2, 1, 2, 0]]
        result = rx.cook(self.ingredients, self.groceries)
        assert result.result
        assert result.medianOffsets == [2, 1]

    def test_hard_cap_at_five(self):
        maxIterations = Config["calibration.diffraction.maximumIterations"]
        rx = Recipe()
        rx.mantidSnapper = mock.Mock()
        rx.mantidSnapper.GroupedDetectorIDs.return_value = {}
        rx.mantidSnapper.OffsetStatistics.side_effect = [{"medianOffset": x} for x in range(10 * maxIterations, 5, -1)]
        result = rx.cook(self.ingredients, self.groceries)
        assert result.result
        assert result.medianOffsets == list(range(10 * maxIterations, 9 * maxIterations, -1))
        # change the config then run again
        maxIterations = 7
        Config._config["calibration"]["diffraction"]["maximumIterations"] = maxIterations
        rx._counts = 0
        rx.mantidSnapper.OffsetStatistics.side_effect = [{"medianOffset": x} for x in range(10 * maxIterations, 5, -1)]
        result = rx.cook(self.ingredients, self.groceries)
        assert result.result
        assert result.medianOffsets == list(range(10 * maxIterations, 9 * maxIterations, -1))

    ## TESTS OF REFERENCE PIXELS

    def test_reference_pixel_consecutive_even(self):
        """Test that the selected reference pixel is always a member of a consecutive, even-order detector group."""
        rx = Recipe()
        gidss = [
            (0, 1),
            (1, 0),
            (0, 1, 2, 3),
            (0, 3, 1, 2),
            (1, 2, 3, 4),
            (1, 4, 3, 2),
        ]
        for gids in gidss:
            assert rx.getRefID(gids) in gids

    def test_reference_pixel_consecutive_odd(self):
        """Test that the selected reference pixel is always a member of a consecutive, odd-order detector group."""
        rx = Recipe()
        gidss = [
            (0,),
            (1,),
            (2,),
            (3,),
            (0, 1, 2),
            (0, 2, 1),
        ]
        for gids in gidss:
            assert rx.getRefID(gids) in gids

    def test_reference_pixel_nonconsecutive_even(self):
        """Test that the selected reference pixel is always a member of a nonconsecutive, even-order detector group."""
        rx = Recipe()
        gidss = [
            (0, 2),
            (0, 3),
            (0, 4),
            (0, 4, 6, 8),
            (0, 4, 7, 1),
            (0, 4, 7, 2),
        ]
        for gids in gidss:
            assert rx.getRefID(gids) in gids

    def test_reference_pixel_nonconsecutive_odd(self):
        """Test that the selected reference pixel is always a member of a nonconsecutive, odd-order detector group."""
        rx = Recipe()
        gidss = [
            (0, 1, 3),
            (4, 8, 3),
            (9, 3, 5),
        ]
        for gids in gidss:
            assert rx.getRefID(gids) in gids

    def test_reference_pixel_selection(self):
        """Test that the selected reference pixel is always a member of the detector group."""
        rx = Recipe()

        # Test even and odd order groups;
        #   test consecutive and non-consecutive groups.
        # Do not test empty groups.
        N_detectors = 10
        dids = list(range(N_detectors))
        for N_group in range(1, 5):
            for gids in permutations(dids, N_group):
                assert rx.getRefID(gids) in gids

    ## TESTS OF MASKING

    def test_mask_is_created(self):
        """Test that a mask workspace is created if it doesn't already exist"""

        uniquePrefix = "test_mic_"
        maskWSName = uniquePrefix + "_mask"
        # Ensure that the mask workspace doesn't already exist
        assert maskWSName not in mtd

        groceries = self.groceries.copy()
        groceries["maskWorkspace"] = maskWSName

        rx = Recipe()
        rx.prep(self.ingredients, groceries)
        rx.execute()
        assert maskWSName in mtd

    def test_existing_mask_is_used(self):
        """Test that an existing mask workspace is not overwritten"""

        rx = Recipe()
        rx.prep(self.ingredients, self.groceries)

        assert self.groceries["maskWorkspace"] in mtd
        # Using 'id' here doesn't work for some reason
        #   so a title is given to the workspace instead.
        mask = mtd[self.groceries["maskWorkspace"]]
        maskTitle = "d1baefaf-d9b4-40db-8f4d-4249a3d3c11b"
        mask.setTitle(maskTitle)

        rx.execute()
        assert self.groceries["maskWorkspace"] in mtd
        mask = mtd[self.groceries["maskWorkspace"]]
        assert mask.getTitle() == maskTitle

    def test_none_are_masked(self):
        """Test that no synthetic spectra are masked"""
        # Success of this test validates the synthetic data used by the other tests.

        rx = Recipe()
        rx.prep(self.ingredients, self.groceries)
        assert self.groceries["maskWorkspace"] in mtd
        mask = mtd[self.groceries["maskWorkspace"]]
        assert mask.getNumberMasked() == 0

        rx.execute()
        assert mask.getNumberMasked() == 0

    def countDetectorsForSpectra(self, inputWS: MatrixWorkspace, nss: Sequence[int]):
        count = 0
        for ns in nss:
            count += len(inputWS.getSpectrum(ns).getDetectorIDs())
        return count

    def prepareSpectraToFail(self, inputWS: MatrixWorkspace, nss: Sequence[int]):
        setSpectraToZero(inputWS, nss)

    def test_failures_are_masked(self):
        """Test that failing spectra are masked"""

        rx = Recipe()
        rx.prep(self.ingredients, self.groceries)

        maskWS = mtd[self.groceries["maskWorkspace"]]
        assert maskWS.getNumberMasked() == 0
        fakeRawData = mtd[self.groceries["inputWorkspace"]]
        # WARNING: Do _not_ zero the reference spectra:
        #   in that case, the entire group corresponding to the reference will fail.
        spectraToFail = (2, 5, 12)
        self.prepareSpectraToFail(fakeRawData, spectraToFail)
        # Verify that at least some spectra have been modified.
        assert self.countDetectorsForSpectra(fakeRawData, spectraToFail) > 0

        rx.execute()
        assert maskWS.getNumberMasked() == self.countDetectorsForSpectra(fakeRawData, spectraToFail)
        for ns in spectraToFail:
            dets = fakeRawData.getSpectrum(ns).getDetectorIDs()
            for det in dets:
                assert maskWS.isMasked(det)

    def test_masks_stay_masked(self):
        """Test that incoming masked spectra are still masked at output"""

        rx = Recipe()
        rx.prep(self.ingredients, self.groceries)

        maskWS = mtd[self.groceries["maskWorkspace"]]
        assert maskWS.getNumberMasked() == 0

        fakeRawData = mtd[self.groceries["inputWorkspace"]]
        spectraToMask = (1, 4, 6, 7)
        maskSpectra(maskWS, fakeRawData, spectraToMask)
        # Verify that at least some detectors have been masked.
        assert self.countDetectorsForSpectra(fakeRawData, spectraToMask) > 0

        rx.execute()
        assert maskWS.getNumberMasked() == self.countDetectorsForSpectra(fakeRawData, spectraToMask)
        for ns in spectraToMask:
            dets = fakeRawData.getSpectrum(ns).getDetectorIDs()
            for det in dets:
                assert maskWS.isMasked(det)

    def test_masks_are_combined(self):
        """Test that masks for failing spectra are combined with any input mask"""

        rx = Recipe()
        rx.prep(self.ingredients, self.groceries)

        maskWS = mtd[self.groceries["maskWorkspace"]]
        assert maskWS.getNumberMasked() == 0

        fakeRawData = mtd[self.groceries["inputWorkspace"]]
        # WARNING: see note at 'test_failures_are_masked'
        spectraToFail = (2, 5, 12)
        self.prepareSpectraToFail(fakeRawData, spectraToFail)
        # Verify that at least some spectra have been modified.
        assert self.countDetectorsForSpectra(fakeRawData, spectraToFail) > 0
        # WARNING: do not overlap with the _failed_ spectra, unless the assertion count is corrected
        spectraToMask = (1, 4, 6, 7)
        maskSpectra(maskWS, fakeRawData, spectraToMask)
        # Verify that at least some detectors have been masked.
        assert self.countDetectorsForSpectra(fakeRawData, spectraToMask) > 0

        rx.execute()
        assert maskWS.getNumberMasked() == self.countDetectorsForSpectra(
            fakeRawData, spectraToFail
        ) + self.countDetectorsForSpectra(fakeRawData, spectraToMask)
        for ns in spectraToFail:
            dets = fakeRawData.getSpectrum(ns).getDetectorIDs()
            for det in dets:
                assert maskWS.isMasked(det)
        for ns in spectraToMask:
            dets = fakeRawData.getSpectrum(ns).getDetectorIDs()
            for det in dets:
                assert maskWS.isMasked(det)
