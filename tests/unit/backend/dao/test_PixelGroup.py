# ruff: noqa: E722, PT011, PT012

import unittest

import pytest

from snapred.backend.dao.Limit import BinnedValue, Limit
from snapred.backend.dao.state.FocusGroup import FocusGroup
from snapred.backend.dao.state.PixelGroup import PixelGroup
from snapred.backend.dao.state.PixelGroupingParameters import PixelGroupingParameters
from snapred.meta.Config import Resource


class TestPixelGroup(unittest.TestCase):
    # we only need this workspace made once for entire test suite
    @classmethod
    def setUpClass(cls):
        cls.nBinsAcrossPeakWidth = 7
        cls.groupIDs = [2, 3, 7, 11]
        cls.isMasked = [False, False, False, False]
        cls.L2 = [10.0, 10.0, 10.0, 10.0]
        cls.twoTheta = [0.1, 0.2, 0.3, 0.4]
        cls.azimuth = [0.0, 0.0, 0.0, 0.0]
        cls.dResolution = [
            Limit(minimum=1.0e-3, maximum=1.0),
            Limit(minimum=1.0e-3, maximum=2.0),
            Limit(minimum=1.0e-4, maximum=1.5),
            Limit(minimum=1.0e-4, maximum=1.0),
        ]
        cls.dRelativeResolution = [0.03, 0.05, 0.07, 0.09]
        cls.pixelGroupingParametersList = [
            PixelGroupingParameters(
                groupID=cls.groupIDs[i],
                isMasked=False,
                L2=10.0,
                twoTheta=cls.twoTheta[i],
                azimuth=0.0,
                dResolution=cls.dResolution[i],
                dRelativeResolution=cls.dRelativeResolution[i],
            )
            for i in range(len(cls.groupIDs))
        ]
        cls.pixelGroupingParameters = {
            cls.groupIDs[i]: cls.pixelGroupingParametersList[i] for i in range(len(cls.groupIDs))
        }

        cls.tofParams = BinnedValue(
            minimum=0,
            maximum=100,
            binWidth=0.03 / cls.nBinsAcrossPeakWidth,
        )

        cls.focusGroup = FocusGroup(
            name="Natural",
            definition=Resource.getPath("inputs/testInstrument/fakeSNAPFocGroup_Natural.xml"),
        )

        try:
            cls.reference = PixelGroup(
                pixelGroupingParameters=cls.pixelGroupingParameters,
                nBinsAcrossPeakWidth=cls.nBinsAcrossPeakWidth,
                timeOfFlight=cls.tofParams,
                focusGroup=cls.focusGroup,
            )
        except:
            pytest.fail("Failed to make a pixel group from a dictionary of PGPs")
        return super().setUpClass()

    def test_init_from_list_of_pgps(self):
        # init from number, list of PGPs
        try:
            pg = PixelGroup(
                pixelGroupingParameters=self.pixelGroupingParametersList,
                nBinsAcrossPeakWidth=self.nBinsAcrossPeakWidth,
                timeOfFlight=self.tofParams,
                focusGroup=self.focusGroup,
            )
        except:
            pytest.fail("Failed to make a pixel group from a list of PGPs")
        assert pg == self.reference

    def test_init_from_lists_of_things(self):
        try:
            pg = PixelGroup(
                groupIDs=self.groupIDs,
                isMasked=self.isMasked,
                L2=self.L2,
                twoTheta=self.twoTheta,
                azimuth=self.azimuth,
                dResolution=self.dResolution,
                dRelativeResolution=self.dRelativeResolution,
                nBinsAcrossPeakWidth=self.nBinsAcrossPeakWidth,
                timeOfFlight=self.tofParams,
                focusGroup=self.focusGroup,
            )
        except:
            pytest.fail("Failed to make a pixel group from base ingredients")
        assert pg == self.reference

    # test getter'ed properties

    def test_get_groupID(self):
        pg = PixelGroup(
            pixelGroupingParameters=self.pixelGroupingParameters,
            nBinsAcrossPeakWidth=self.nBinsAcrossPeakWidth,
            timeOfFlight=self.tofParams,
            focusGroup=self.focusGroup,
        )
        assert pg.groupIDs == self.groupIDs

    def test_get_twoTheta(self):
        pg = PixelGroup(
            pixelGroupingParameters=self.pixelGroupingParameters,
            nBinsAcrossPeakWidth=self.nBinsAcrossPeakWidth,
            timeOfFlight=self.tofParams,
            focusGroup=self.focusGroup,
        )
        assert pg.twoTheta == self.twoTheta

    def test_get_dResolution(self):
        pg = PixelGroup(
            pixelGroupingParameters=self.pixelGroupingParameters,
            nBinsAcrossPeakWidth=self.nBinsAcrossPeakWidth,
            timeOfFlight=self.tofParams,
            focusGroup=self.focusGroup,
        )
        assert pg.dResolution == self.dResolution

    def test_get_dRelativeResolution(self):
        pg = PixelGroup(
            pixelGroupingParameters=self.pixelGroupingParameters,
            nBinsAcrossPeakWidth=self.nBinsAcrossPeakWidth,
            timeOfFlight=self.tofParams,
            focusGroup=self.focusGroup,
        )
        assert pg.dRelativeResolution == self.dRelativeResolution

    # test getter methods

    def test_get_dMax(self):
        pg = PixelGroup(
            pixelGroupingParameters=self.pixelGroupingParameters,
            nBinsAcrossPeakWidth=self.nBinsAcrossPeakWidth,
            timeOfFlight=self.tofParams,
            focusGroup=self.focusGroup,
        )
        assert pg.dMax() == [dl.maximum for dl in self.dResolution]

    def test_get_dMin(self):
        pg = PixelGroup(
            pixelGroupingParameters=self.pixelGroupingParameters,
            nBinsAcrossPeakWidth=self.nBinsAcrossPeakWidth,
            timeOfFlight=self.tofParams,
            focusGroup=self.focusGroup,
        )
        assert pg.dMin() == [dl.minimum for dl in self.dResolution]

    def test_get_dBin_LOG(self):
        pg = PixelGroup(
            pixelGroupingParameters=self.pixelGroupingParameters,
            nBinsAcrossPeakWidth=self.nBinsAcrossPeakWidth,
            timeOfFlight=self.tofParams,
            focusGroup=self.focusGroup,
            binningMode=PixelGroup.BinningMode.LOG,
        )
        binWidths = pg.dBin()
        assert binWidths == [-abs(dl / self.nBinsAcrossPeakWidth) for dl in self.dRelativeResolution]
        for bw in binWidths:
            assert bw <= 0.0

    def test_get_dBin_LINEAR(self):
        pg = PixelGroup(
            pixelGroupingParameters=self.pixelGroupingParameters,
            nBinsAcrossPeakWidth=self.nBinsAcrossPeakWidth,
            timeOfFlight=self.tofParams,
            focusGroup=self.focusGroup,
            binningMode=PixelGroup.BinningMode.LINEAR,
        )
        binWidths = pg.dBin()
        assert binWidths == [abs(dl / 7) for dl in self.dRelativeResolution]
        for bw in binWidths:
            assert bw >= 0.0

    def test_operator_access(self):
        for gid in self.groupIDs:
            assert self.reference[gid] == self.reference.pixelGroupingParameters[gid]
            assert self.reference.groupIDs != self.reference.pixelGroupingParameters[gid]

    def test_init(self):
        p = PixelGroupingParameters.model_validate(self.reference[2])
        assert p == self.reference[2]
