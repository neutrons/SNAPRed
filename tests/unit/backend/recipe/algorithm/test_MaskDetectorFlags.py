from mantid.simpleapi import (
    CloneWorkspace,
    LoadDetectorsGroupingFile,
    LoadEmptyInstrument,
    mtd,
)
from snapred.backend.log.logger import snapredLogger

# the algorithm to test
from snapred.backend.recipe.algorithm.MaskDetectorFlags import MaskDetectorFlags
from snapred.meta.Config import Resource
from util.helpers import (
    createCompatibleMask,
    deleteWorkspaceNoThrow,
    workspacesEqual,
)

logger = snapredLogger.getLogger(__name__)


class TestMaskDetectorFlags:
    @classmethod
    def setup_class(cls):
        instrumentFilename = Resource.getPath("inputs/testInstrument/fakeSNAP.xml")
        groupingFilename = Resource.getPath("inputs/testInstrument/fakeSNAPFocGroup_Column.xml")
        cls.instrumentWS = "test_instrument_workspace"
        cls.maskWS = "test_mask"
        cls.groupingWS = "test_grouping"
        LoadEmptyInstrument(
            Filename=instrumentFilename,
            OutputWorkspace=cls.instrumentWS,
        )
        createCompatibleMask(cls.maskWS, cls.instrumentWS, instrumentFilename)
        LoadDetectorsGroupingFile(
            InputFile=groupingFilename,
            InputWorkspace=cls.instrumentWS,
            OutputWorkspace=cls.groupingWS,
        )

        cls.exclude = [cls.instrumentWS, cls.maskWS, cls.groupingWS]

    @classmethod
    def teardown_class(cls):
        mtd.clear()

    def setup_method(self):
        self.testInstrumentWS = mtd.unique_hidden_name()
        self.testMaskWS = mtd.unique_hidden_name()
        self.testGroupingWS = mtd.unique_hidden_name()
        CloneWorkspace(
            InputWorkspace=self.instrumentWS,
            OutputWorkspace=self.testInstrumentWS,
        )
        CloneWorkspace(
            InputWorkspace=self.maskWS,
            OutputWorkspace=self.testMaskWS,
        )
        CloneWorkspace(
            InputWorkspace=self.groupingWS,
            OutputWorkspace=self.testGroupingWS,
        )

    def teardown_method(self):
        for ws in mtd.getObjectNames():
            if ws not in self.exclude:
                deleteWorkspaceNoThrow(ws)

    def test_init_properties(self):
        """Test that the properties of the algorithm can be initialized"""
        algo = MaskDetectorFlags()
        algo.initialize()
        algo.setProperty("MaskWorkspace", self.testMaskWS)
        algo.setProperty("OutputWorkspace", self.testInstrumentWS)
        assert algo.getPropertyValue("MaskWorkspace") == self.testMaskWS
        assert algo.getPropertyValue("OutputWorkspace") == self.testInstrumentWS

    def test_exec(self):
        """Test that the detector flags are set to match the mask values"""
        mask = mtd[self.testMaskWS]
        assert mask.getNumberMasked() == 0
        # mask _odd_ detector ids
        detectors = mask.detectorInfo()
        ids = mask.detectorInfo().detectorIDs()
        maskedCount = 0
        for id_ in ids:
            index = detectors.indexOf(int(id_))
            if not detectors.isMonitor(index) and id_ % 2 != 0:
                mask.setValue(int(id_), True)
                maskedCount += 1
        assert mask.getNumberMasked() == maskedCount

        algo = MaskDetectorFlags()
        algo.initialize()
        algo.setProperty("MaskWorkspace", self.testMaskWS)
        algo.setProperty("OutputWorkspace", self.testInstrumentWS)

        assert algo.execute()
        test = mtd[self.testInstrumentWS]
        testDetectors = test.detectorInfo()
        flag = True
        testCount = 0
        for id_ in ids:
            index = detectors.indexOf(int(id_))
            if not detectors.isMonitor(index):
                maskFlag = testDetectors.isMasked(index)
                if maskFlag:
                    testCount += 1
                if maskFlag != mask.isMasked(int(id_)):
                    flag = False
                    break
        assert testCount == maskedCount
        assert flag

    def test_exec_no_clear(self):
        """Test that no workspace values are modified"""
        mask = mtd[self.testMaskWS]
        assert mask.getNumberMasked() == 0
        # mask _odd_ detector ids
        detectors = mask.detectorInfo()
        ids = mask.detectorInfo().detectorIDs()
        maskedCount = 0
        for id_ in ids:
            index = detectors.indexOf(int(id_))
            if not detectors.isMonitor(index) and id_ % 2 != 0:
                mask.setValue(int(id_), True)
                maskedCount += 1
        assert mask.getNumberMasked() == maskedCount

        algo = MaskDetectorFlags()
        algo.initialize()
        algo.setProperty("MaskWorkspace", self.testMaskWS)
        algo.setProperty("OutputWorkspace", self.testInstrumentWS)

        algo.execute()
        assert workspacesEqual(
            Workspace1=self.testInstrumentWS,
            Workspace2=self.instrumentWS,
            CheckInstrument=False,
        )

    def test_exec_with_group(self):
        """Test that a grouping-workspace's detector flags are set"""
        mask = mtd[self.testMaskWS]
        assert mask.getNumberMasked() == 0
        # mask _odd_ detector ids
        detectors = mask.detectorInfo()
        ids = mask.detectorInfo().detectorIDs()
        maskedCount = 0
        for id_ in ids:
            index = detectors.indexOf(int(id_))
            if not detectors.isMonitor(index) and id_ % 2 != 0:
                mask.setValue(int(id_), True)
                maskedCount += 1
        assert mask.getNumberMasked() == maskedCount

        algo = MaskDetectorFlags()
        algo.initialize()
        algo.setProperty("MaskWorkspace", self.testMaskWS)
        algo.setProperty("OutputWorkspace", self.testGroupingWS)

        algo.execute()
        test = mtd[self.testGroupingWS]
        testDetectors = test.detectorInfo()
        flag = True
        testCount = 0
        for id_ in ids:
            index = detectors.indexOf(int(id_))
            if not detectors.isMonitor(index):
                maskFlag = testDetectors.isMasked(index)
                if maskFlag:
                    testCount += 1
                if maskFlag != mask.isMasked(int(id_)):
                    flag = False
                    break
        assert testCount == maskedCount
        assert flag

    def test_exec_with_group_no_clear(self):
        """Test that a grouping-workspace's values are not modified"""
        mask = mtd[self.testMaskWS]
        assert mask.getNumberMasked() == 0
        # mask _odd_ detector ids
        detectors = mask.detectorInfo()
        ids = mask.detectorInfo().detectorIDs()
        maskedCount = 0
        for id_ in ids:
            index = detectors.indexOf(int(id_))
            if not detectors.isMonitor(index) and id_ % 2 != 0:
                mask.setValue(int(id_), True)
                maskedCount += 1
        assert mask.getNumberMasked() == maskedCount

        algo = MaskDetectorFlags()
        algo.initialize()
        algo.setProperty("MaskWorkspace", self.testMaskWS)
        algo.setProperty("OutputWorkspace", self.testGroupingWS)

        algo.execute()
        assert workspacesEqual(
            Workspace1=self.testGroupingWS,
            Workspace2=self.groupingWS,
            CheckInstrument=False,
        )

    def test_exec_with_other_mask(self):
        """Test that a mask workspace's detector flags are set"""
        testOtherMaskWS = mtd.unique_hidden_name()
        CloneWorkspace(
            InputWorkspace=self.maskWS,
            OutputWorkspace=testOtherMaskWS,
        )
        assert mtd[testOtherMaskWS].getNumberMasked() == 0

        mask = mtd[self.testMaskWS]
        assert mask.getNumberMasked() == 0
        # mask _odd_ detector ids
        detectors = mask.detectorInfo()
        ids = mask.detectorInfo().detectorIDs()
        maskedCount = 0
        for id_ in ids:
            index = detectors.indexOf(int(id_))
            if not detectors.isMonitor(index) and id_ % 2 != 0:
                mask.setValue(int(id_), True)
                maskedCount += 1
        assert mask.getNumberMasked() == maskedCount

        algo = MaskDetectorFlags()
        algo.initialize()
        algo.setProperty("MaskWorkspace", self.testMaskWS)
        algo.setProperty("OutputWorkspace", testOtherMaskWS)

        algo.execute()
        test = mtd[testOtherMaskWS]
        testDetectors = test.detectorInfo()
        flag = True
        testCount = 0
        for id_ in ids:
            index = detectors.indexOf(int(id_))
            if not detectors.isMonitor(index):
                maskFlag = testDetectors.isMasked(index)
                if maskFlag:
                    testCount += 1
                if maskFlag != mask.isMasked(int(id_)):
                    flag = False
                    break
        assert testCount == maskedCount
        assert flag

    def test_exec_with_other_mask_no_clear(self):
        """Test that a mask workspace's values are not modified"""
        testOtherMaskWS = mtd.unique_hidden_name()
        CloneWorkspace(
            InputWorkspace=self.maskWS,
            OutputWorkspace=testOtherMaskWS,
        )
        assert mtd[testOtherMaskWS].getNumberMasked() == 0

        mask = mtd[self.testMaskWS]
        assert mask.getNumberMasked() == 0
        # mask _odd_ detector ids
        detectors = mask.detectorInfo()
        ids = mask.detectorInfo().detectorIDs()
        maskedCount = 0
        for id_ in ids:
            index = detectors.indexOf(int(id_))
            if not detectors.isMonitor(index) and id_ % 2 != 0:
                mask.setValue(int(id_), True)
                maskedCount += 1
        assert mask.getNumberMasked() == maskedCount

        algo = MaskDetectorFlags()
        algo.initialize()
        algo.setProperty("MaskWorkspace", self.testMaskWS)
        algo.setProperty("OutputWorkspace", testOtherMaskWS)

        algo.execute()
        assert workspacesEqual(
            Workspace1=testOtherMaskWS,
            Workspace2=self.maskWS,
            CheckInstrument=False,
        )
