import json
import unittest
from collections.abc import Sequence
from itertools import permutations

import pytest
from mantid.api import MatrixWorkspace
from mantid.simpleapi import mtd

# needed to make mocked ingredients
# the algorithm to test
from snapred.backend.recipe.algorithm.PixelDiffractionCalibration import (
    PixelDiffractionCalibration as ThisAlgo,  # noqa: E402
)
from util.diffraction_calibration_synthetic_data import SyntheticData
from util.helpers import maskSpectra, setSpectraToZero


class TestPixelDiffractionCalibration(unittest.TestCase):
    def setUp(self):
        """Create a set of mocked ingredients for calculating DIFC corrected by offsets"""
        inputs = SyntheticData()
        self.fakeIngredients = inputs.ingredients

        runNumber = self.fakeIngredients.runConfig.runNumber
        self.fakeRawData = f"_test_pixelcal_{runNumber}"
        self.fakeGroupingWorkspace = f"_test_pixelcal_difc_{runNumber}"
        self.fakeMaskWorkspace = f"_test_pixelcal_difc_{runNumber}_mask"
        inputs.generateWorkspaces(self.fakeRawData, self.fakeGroupingWorkspace, self.fakeMaskWorkspace)

    def tearDown(self) -> None:
        # At present tests are not run in parallel, so cleanup the ADS:
        mtd.clear()
        assert len(mtd.getObjectNames()) == 0
        return super().tearDown()

    def test_chop_ingredients(self):
        """Test that ingredients for algo are properly processed"""
        algo = ThisAlgo()
        algo.initialize()
        algo.chopIngredients(self.fakeIngredients)
        assert algo.runNumber == self.fakeIngredients.runConfig.runNumber
        assert algo.overallDMin == min(self.fakeIngredients.pixelGroup.dMin())
        assert algo.overallDMax == max(self.fakeIngredients.pixelGroup.dMax())
        assert algo.dBin == max([abs(db) for db in self.fakeIngredients.pixelGroup.dBin()])

    def test_init_properties(self):
        """Test that the properties of the algorithm can be initialized"""
        algo = ThisAlgo()
        algo.initialize()
        algo.setProperty("Ingredients", self.fakeIngredients.json())
        assert algo.getProperty("Ingredients").value == self.fakeIngredients.json()

    def test_execute(self):
        """Test that the algorithm executes"""
        algo = ThisAlgo()
        algo.initialize()
        algo.setProperty("InputWorkspace", self.fakeRawData)
        algo.setProperty("MaskWorkspace", self.fakeMaskWorkspace)
        algo.setProperty("Groupingworkspace", self.fakeGroupingWorkspace)
        algo.setProperty("Ingredients", self.fakeIngredients.json())
        assert algo.execute()

        data = json.loads(algo.getProperty("data").value)
        x = data["medianOffset"]
        assert x is not None
        assert x != 0.0
        assert x > 0.0
        assert x <= self.fakeIngredients.maxOffset

    # patch to make the offsets of sample data non-zero
    def test_reexecution_and_convergence(self):
        """Test that the algorithm can run, and that it will converge to an answer"""
        algo = ThisAlgo()
        algo.initialize()
        algo.setProperty("InputWorkspace", self.fakeRawData)
        algo.setProperty("MaskWorkspace", self.fakeMaskWorkspace)
        algo.setProperty("Groupingworkspace", self.fakeGroupingWorkspace)
        algo.setProperty("Ingredients", self.fakeIngredients.json())
        assert algo.execute()

        data = json.loads(algo.getProperty("data").value)
        x = data["medianOffset"]
        assert x is not None
        assert x != 0.0
        assert x > 0.0
        assert x <= self.fakeIngredients.maxOffset

        # check that value converges
        # WARNING: testing for three iterations seems to be about the limit here.
        #   At greater than 3 iterations, there are small oscillations about a limit value.
        maxIter = 3
        allOffsets = [data["medianOffset"]]

        # The following assertion will fail if the convergence behavior is oversimplified:
        #   an initial fast convergence (two iterations or so) followed by minor oscillations
        #   should still be considered to be a passing result.
        for i in range(maxIter - 1):
            algo.execute()
            data = json.loads(algo.getProperty("data").value)
            allOffsets.append(data["medianOffset"])
            assert allOffsets[-1] <= max(1.0e-4, allOffsets[-2])

    def test_reference_pixel_consecutive_even(self):
        """Test that the selected reference pixel is always a member of a consecutive, even-order detector group."""
        algo = ThisAlgo()
        gidss = [
            (0, 1),
            (1, 0),
            (0, 1, 2, 3),
            (0, 3, 1, 2),
            (1, 2, 3, 4),
            (1, 4, 3, 2),
        ]
        for gids in gidss:
            assert algo.getRefID(gids) in gids

    def test_reference_pixel_consecutive_odd(self):
        """Test that the selected reference pixel is always a member of a consecutive, odd-order detector group."""
        algo = ThisAlgo()
        gidss = [
            (0,),
            (1,),
            (2,),
            (3,),
            (0, 1, 2),
            (0, 2, 1),
        ]
        for gids in gidss:
            assert algo.getRefID(gids) in gids

    def test_reference_pixel_nonconsecutive_even(self):
        """Test that the selected reference pixel is always a member of a nonconsecutive, even-order detector group."""
        algo = ThisAlgo()
        gidss = [
            (0, 2),
            (0, 3),
            (0, 4),
            (0, 4, 6, 8),
            (0, 4, 7, 1),
            (0, 4, 7, 2),
        ]
        for gids in gidss:
            assert algo.getRefID(gids) in gids

    def test_reference_pixel_nonconsecutive_odd(self):
        """Test that the selected reference pixel is always a member of a nonconsecutive, odd-order detector group."""
        algo = ThisAlgo()
        gidss = [
            (0, 1, 3),
            (4, 8, 3),
            (9, 3, 5),
        ]
        for gids in gidss:
            assert algo.getRefID(gids) in gids

    def test_reference_pixel_selection(self):
        """Test that the selected reference pixel is always a member of the detector group."""
        algo = ThisAlgo()

        # Test even and odd order groups;
        #   test consecutive and non-consecutive groups.
        # Do not test empty groups.
        N_detectors = 10
        dids = list(range(N_detectors))
        for N_group in range(1, 5):
            for gids in permutations(dids, N_group):
                assert algo.getRefID(gids) in gids

    def test_mask_is_created(self):
        """Test that a mask workspace is created if it doesn't already exist"""

        uniquePrefix = "test_mic_"
        maskWSName = uniquePrefix + "_mask"
        # Ensure that the mask workspace doesn't already exist
        assert maskWSName not in mtd

        algo = ThisAlgo()
        algo.initialize()
        algo.setProperty("InputWorkspace", self.fakeRawData)
        algo.setPropertyValue("MaskWorkspace", maskWSName)
        algo.setProperty("Groupingworkspace", self.fakeGroupingWorkspace)
        algo.setProperty("Ingredients", self.fakeIngredients.json())
        algo.execute()
        assert maskWSName in mtd

    def test_existing_mask_is_used(self):
        """Test that an existing mask workspace is not overwritten"""

        algo = ThisAlgo()
        algo.initialize()
        algo.setProperty("InputWorkspace", self.fakeRawData)
        algo.setProperty("MaskWorkspace", self.fakeMaskWorkspace)
        algo.setProperty("Groupingworkspace", self.fakeGroupingWorkspace)
        algo.setProperty("Ingredients", self.fakeIngredients.json())

        assert self.fakeMaskWorkspace in mtd
        # Using 'id' here doesn't work for some reason
        #   so a title is given to the workspace instead.
        mask = mtd[self.fakeMaskWorkspace]
        maskTitle = "d1baefaf-d9b4-40db-8f4d-4249a3d3c11b"
        mask.setTitle(maskTitle)

        algo.execute()
        assert self.fakeMaskWorkspace in mtd
        mask = mtd[self.fakeMaskWorkspace]
        assert mask.getTitle() == maskTitle

    def test_none_are_masked(self):
        """Test that no synthetic spectra are masked"""
        # Success of this test validates the synthetic data used by the other tests.

        algo = ThisAlgo()
        algo.initialize()
        algo.setProperty("InputWorkspace", self.fakeRawData)
        algo.setProperty("MaskWorkspace", self.fakeMaskWorkspace)
        algo.setProperty("Groupingworkspace", self.fakeGroupingWorkspace)
        algo.setProperty("Ingredients", self.fakeIngredients.json())
        assert self.fakeMaskWorkspace in mtd
        mask = mtd[self.fakeMaskWorkspace]
        assert mask.getNumberMasked() == 0

        algo.execute()
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

        algo = ThisAlgo()
        algo.initialize()
        algo.setProperty("InputWorkspace", self.fakeRawData)
        algo.setProperty("MaskWorkspace", self.fakeMaskWorkspace)
        algo.setProperty("Groupingworkspace", self.fakeGroupingWorkspace)
        algo.setProperty("Ingredients", self.fakeIngredients.json())

        maskWS = mtd[self.fakeMaskWorkspace]
        assert maskWS.getNumberMasked() == 0
        fakeRawData = mtd[self.fakeRawData]
        # WARNING: Do _not_ zero the reference spectra:
        #   in that case, the entire group corresponding to the reference will fail.
        spectraToFail = (2, 5, 12)
        self.prepareSpectraToFail(fakeRawData, spectraToFail)
        # Verify that at least some spectra have been modified.
        assert self.countDetectorsForSpectra(fakeRawData, spectraToFail) > 0

        algo.execute()
        assert maskWS.getNumberMasked() == self.countDetectorsForSpectra(fakeRawData, spectraToFail)
        for ns in spectraToFail:
            dets = fakeRawData.getSpectrum(ns).getDetectorIDs()
            for det in dets:
                assert maskWS.isMasked(det)

    def test_masks_stay_masked(self):
        """Test that incoming masked spectra are still masked at output"""

        algo = ThisAlgo()
        algo.initialize()
        algo.setProperty("InputWorkspace", self.fakeRawData)
        algo.setProperty("MaskWorkspace", self.fakeMaskWorkspace)
        algo.setProperty("Groupingworkspace", self.fakeGroupingWorkspace)
        algo.setProperty("Ingredients", self.fakeIngredients.json())

        maskWS = mtd[self.fakeMaskWorkspace]
        assert maskWS.getNumberMasked() == 0

        fakeRawData = mtd[self.fakeRawData]
        spectraToMask = (1, 4, 6, 7)
        maskSpectra(maskWS, fakeRawData, spectraToMask)
        # Verify that at least some detectors have been masked.
        assert self.countDetectorsForSpectra(fakeRawData, spectraToMask) > 0

        algo.execute()
        assert maskWS.getNumberMasked() == self.countDetectorsForSpectra(fakeRawData, spectraToMask)
        for ns in spectraToMask:
            dets = fakeRawData.getSpectrum(ns).getDetectorIDs()
            for det in dets:
                assert maskWS.isMasked(det)

    def test_masks_are_combined(self):
        """Test that masks for failing spectra are combined with any input mask"""

        algo = ThisAlgo()
        algo.initialize()
        algo.setProperty("InputWorkspace", self.fakeRawData)
        algo.setProperty("MaskWorkspace", self.fakeMaskWorkspace)
        algo.setProperty("Groupingworkspace", self.fakeGroupingWorkspace)
        algo.setProperty("Ingredients", self.fakeIngredients.json())

        maskWS = mtd[self.fakeMaskWorkspace]
        assert maskWS.getNumberMasked() == 0

        fakeRawData = mtd[self.fakeRawData]
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

        algo.execute()
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


# this at teardown removes the loggers, eliminating logger error printouts
# see https://github.com/pytest-dev/pytest/issues/5502#issuecomment-647157873
@pytest.fixture(autouse=True)
def clear_loggers():  # noqa: PT004
    """Remove handlers from all loggers"""
    import logging

    yield  # teardown follows: ...
    loggers = [logging.getLogger()] + list(logging.Logger.manager.loggerDict.values())
    for logger in loggers:
        handlers = getattr(logger, "handlers", [])
        for handler in handlers:
            logger.removeHandler(handler)
