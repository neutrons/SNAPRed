import tempfile
import unittest
from pathlib import Path

from mantid.simpleapi import (
    CreateSampleWorkspace,
    CreateSingleValuedWorkspace,
    LoadDetectorsGroupingFile,
    LoadInstrument,
    mtd,
)

from snapred.backend.recipe.algorithm.DetectorPanelStraddleCheck import DetectorPanelStraddleCheck as Algo
from snapred.meta.Config import Resource
from snapred.meta.pointer import access_pointer

# Fake SNAP instrument: East = bank1 (IDs 0-3) + bank2 (IDs 4-7)
#                        West = bank3 (IDs 8-11) + bank4 (IDs 12-15)
FAKE_INSTRUMENT = Resource.getPath("inputs/testInstrument/fakeSNAP_Definition.xml")


def _grouping_from_xml(xml_content, raw_ws_name, output_name):
    """Write XML to a temp file and load it as a GroupingWorkspace."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".xml", delete=False) as f:
        f.write(xml_content)
        tmp_path = f.name
    LoadDetectorsGroupingFile(
        InputFile=tmp_path,
        InputWorkspace=raw_ws_name,
        OutputWorkspace=output_name,
    )
    Path(tmp_path).unlink(missing_ok=True)


class TestDetectorPanelStraddleCheck(unittest.TestCase):
    rawWsName = "__test_panel_straddle_raw"

    @classmethod
    def setUpClass(cls):
        CreateSampleWorkspace(
            OutputWorkspace=cls.rawWsName,
            NumBanks=4,
            BankPixelWidth=2,
            XMin=0,
            XMax=1,
            BinWidth=1,
        )
        LoadInstrument(
            Workspace=cls.rawWsName,
            Filename=FAKE_INSTRUMENT,
            RewriteSpectraMap=True,
        )

    @classmethod
    def tearDownClass(cls):
        mtd.clear()

    def test_init(self):
        algo = Algo()
        algo.initialize()

    def test_validate_rejects_non_grouping_workspace(self):
        tmp = mtd.unique_name(5, "strdl")
        CreateSingleValuedWorkspace(OutputWorkspace=tmp)
        algo = Algo()
        algo.initialize()
        algo.setProperty("GroupingWorkspace", tmp)
        err = algo.validateInputs()
        assert "GroupingWorkspace" in err

    def test_all_groups_straddle(self):
        """Every group has detectors on both East and West panels."""
        xml = (
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<detector-grouping instrument="fakeSNAP">'
            '  <group ID="1"><detids>0,5,10,15</detids></group>'
            '  <group ID="2"><detids>1,7,8,13</detids></group>'
            '  <group ID="3"><detids>2,4,11,14</detids></group>'
            '  <group ID="4"><detids>3,6,9,12</detids></group>'
            "</detector-grouping>"
        )
        grpName = "__test_straddle_all"
        _grouping_from_xml(xml, self.rawWsName, grpName)

        algo = Algo()
        algo.initialize()
        algo.setProperty("GroupingWorkspace", grpName)
        assert algo.execute()

        result = access_pointer(algo.getProperty("StraddlingGroups").value)
        assert sorted(result) == [1, 2, 3, 4]

    def test_no_groups_straddle(self):
        """East-only and West-only groups should not be flagged."""
        xml = (
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<detector-grouping instrument="fakeSNAP">'
            '  <group ID="1"><detids>0-7</detids></group>'
            '  <group ID="2"><detids>8-15</detids></group>'
            "</detector-grouping>"
        )
        grpName = "__test_straddle_none"
        _grouping_from_xml(xml, self.rawWsName, grpName)

        algo = Algo()
        algo.initialize()
        algo.setProperty("GroupingWorkspace", grpName)
        assert algo.execute()

        result = access_pointer(algo.getProperty("StraddlingGroups").value)
        assert result == []

    def test_some_groups_straddle(self):
        """Only groups that mix East and West pixels should appear."""
        # Group 1: all East (0-3)
        # Group 2: straddles (East 4-7 + West 8-9)
        # Group 3: all West (10-15)
        xml = (
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<detector-grouping instrument="fakeSNAP">'
            '  <group ID="1"><detids>0,1,2,3</detids></group>'
            '  <group ID="2"><detids>4,5,6,7,8,9</detids></group>'
            '  <group ID="3"><detids>10,11,12,13,14,15</detids></group>'
            "</detector-grouping>"
        )
        grpName = "__test_straddle_some"
        _grouping_from_xml(xml, self.rawWsName, grpName)

        algo = Algo()
        algo.initialize()
        algo.setProperty("GroupingWorkspace", grpName)
        assert algo.execute()

        result = access_pointer(algo.getProperty("StraddlingGroups").value)
        assert result == [2]

    def test_single_group_all_pixels(self):
        """A single group containing all pixels straddles."""
        xml = (
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<detector-grouping instrument="fakeSNAP">'
            '  <group ID="1"><detids>0-15</detids></group>'
            "</detector-grouping>"
        )
        grpName = "__test_straddle_single"
        _grouping_from_xml(xml, self.rawWsName, grpName)

        algo = Algo()
        algo.initialize()
        algo.setProperty("GroupingWorkspace", grpName)
        assert algo.execute()

        result = access_pointer(algo.getProperty("StraddlingGroups").value)
        assert result == [1]
