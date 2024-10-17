import unittest

import pytest
from mantid.simpleapi import (
    CreateEmptyTableWorkspace,
    DeleteWorkspace,
    mtd,
)
from mantid.testing import assert_almost_equal

# the algorithm to test
from snapred.backend.recipe.algorithm.CreateTableWorkspace import (
    CreateTableWorkspace as Algo,  # noqa: E402
)
from snapred.meta.pointer import create_pointer


class TestCreateTableWorkspace(unittest.TestCase):
    def setUp(self):
        self.correct_ints = [1, 2]
        self.correct_doubles = [1.0, 2.0]
        self.correct_strings = ["one", "two"]

        self.ref_wksp = CreateEmptyTableWorkspace(OutputWorkspace="ref_wksp")
        self.ref_wksp.addColumn(type="int", name="int-col")
        self.ref_wksp.addColumn(type="double", name="double-col")
        self.ref_wksp.addColumn(type="str", name="str-col")
        for i in range(len(self.correct_doubles)):
            self.ref_wksp.addRow(
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
        wksp = mtd.unique_name(prefix="createtab_")
        data = {"int-col": self.correct_ints, "double-col": self.correct_doubles, "str-col": self.correct_strings}
        algo = Algo()
        algo.initialize()
        algo.setPropertyValue("OutputWorkspace", wksp)
        algo.setProperty("Data", create_pointer(data))
        algo.execute()

        assert_almost_equal(wksp, self.ref_wksp)
        tab = mtd[wksp]
        assert tab.column("int-col") == self.correct_ints
        assert tab.column("double-col") == self.correct_doubles
        assert tab.column("str-col") == self.correct_strings

    def test_validate_fail_collen(self):
        wksp = mtd.unique_name(prefix="createtab_")
        incorrect_ints = self.correct_ints.copy()
        incorrect_ints.append(5)
        data = {"int-col": incorrect_ints, "double-col": self.correct_doubles, "str-col": self.correct_strings}
        algo = Algo()
        algo.initialize()
        algo.setPropertyValue("OutputWorkspace", wksp)
        algo.setProperty("Data", create_pointer(data))
        with pytest.raises(RuntimeError) as e:
            algo.execute()
        assert "Column mismatch: length" in str(e.value)

    def test_success_inline(self):
        wksp = mtd.unique_name(prefix="createtab_")
        algo = Algo()
        algo.initialize()
        algo.setPropertyValue("OutputWorkspace", wksp)
        algo.setProperty("Data", create_pointer({"col1": [3, 4], "col2": ["x", "a"]}))
        algo.execute()

        tab = mtd[wksp].toDict()
        assert tab["col1"] == [3, 4]
        assert tab["col2"] == ["x", "a"]

    def test_success_nothing(self):
        wksp = mtd.unique_name(prefix="createtab_")
        algo = Algo()
        algo.initialize()
        algo.setPropertyValue("OutputWorkspace", wksp)
        algo.setProperty("Data", create_pointer({"col1": []}))
        algo.execute()

        tab = mtd[wksp].toDict()
        assert tab["col1"] == []
