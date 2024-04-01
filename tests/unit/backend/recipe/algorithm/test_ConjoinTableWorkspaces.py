import unittest

import pytest
from mantid.simpleapi import (
    CreateEmptyTableWorkspace,
    DeleteWorkspace,
    mtd,
)

# the algorithm to test
from snapred.backend.recipe.algorithm.ConjoinTableWorkspaces import (
    ConjoinTableWorkspaces as Algo,  # noqa: E402
)
from snapred.meta.Config import Resource


class TestCalculateDiffCalTable(unittest.TestCase):
    def setUp(self):
        self.correct_ints = [1, 2]
        self.correct_doubles = [1.0, 2.0]
        self.correct_strings = ["one", "two"]

        self.wksp1 = CreateEmptyTableWorkspace(OutputWorkspace="wksp1")
        self.wksp2 = CreateEmptyTableWorkspace(OutputWorkspace="wksp2")
        for i, ws in enumerate([self.wksp1, self.wksp2]):
            ws.addColumn(type="int", name="int-col")
            ws.addColumn(type="double", name="double-col")
            ws.addColumn(type="str", name="str-col")
            ws.addRow(
                {
                    "int-col": self.correct_ints[i],
                    "double-col": self.correct_doubles[i],
                    "str-col": self.correct_strings[i],
                },
            )

    def tearDown(self) -> None:
        for ws in mtd.getObjectNames():
            try:
                DeleteWorkspace(ws)
            except:  # noqa: E722
                pass
        return super().tearDown()

    def test_success(self):
        algo = Algo()
        algo.initialize()
        algo.setPropertyValue("InputWorkspace1", "wksp1")
        algo.setPropertyValue("InputWorkspace2", "wksp2")
        algo.execute()

        assert self.wksp1.column("int-col") == self.correct_ints
        assert self.wksp1.column("double-col") == self.correct_doubles
        assert self.wksp1.column("str-col") == self.correct_strings

    def test_validate_fail_colcount(self):
        self.wksp2.addColumn(type="int", name="extra column")
        self.wksp2.setCell("extra column", 0, 3)
        assert self.wksp2.columnCount() == 4

        algo = Algo()
        algo.initialize()
        algo.setPropertyValue("InputWorkspace1", "wksp1")
        algo.setPropertyValue("InputWorkspace2", "wksp2")
        with pytest.raises(RuntimeError) as e:
            algo.execute()
        assert "mismatched numbers of columns" in str(e.value)

    def test_validate_fail_name(self):
        self.wksp2.removeColumn("str-col")
        self.wksp2.addColumn(type="str", name="bad name")
        self.wksp2.setCell("bad name", 0, "3")

        algo = Algo()
        algo.initialize()
        algo.setPropertyValue("InputWorkspace1", "wksp1")
        algo.setPropertyValue("InputWorkspace2", "wksp2")
        with pytest.raises(RuntimeError) as e:
            algo.execute()
        assert "bad name" in str(e.value)

    def test_validate_fail_type(self):
        self.wksp2.removeColumn("str-col")
        self.wksp2.addColumn(type="int", name="str-col")
        self.wksp2.setCell("str-col", 0, 3)

        algo = Algo()
        algo.initialize()
        algo.setPropertyValue("InputWorkspace1", "wksp1")
        algo.setPropertyValue("InputWorkspace2", "wksp2")
        with pytest.raises(RuntimeError) as e:
            algo.execute()
        assert "column types" in str(e.value)
