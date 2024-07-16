import unittest

from mantid.simpleapi import (
    BufferMissingColumnsAlgo,
    DeleteWorkspace,
    mtd,
)


class TestBufferMissingColumnsAlgo(unittest.TestCase):
    def setUp(self):
        self.commonColumns = [
            "col1",
            "col3",
            "col5",
        ]
        self.workspace1Columns = self.commonColumns.copy()
        self.workspace1Columns.insert(1, "col2")

        self.workspace2Columns = self.commonColumns.copy()
        self.workspace2Columns.insert(2, "col4")

        self.combinedColumns = set(self.workspace1Columns + self.workspace2Columns)
        self.combinedColumns = sorted(self.combinedColumns)

        self.workspace1 = self.generateTableWorkspace("workspace1", self.workspace1Columns)
        self.workspace2 = self.generateTableWorkspace("workspace2", self.workspace2Columns)

    def tearDown(self) -> None:
        DeleteWorkspace(self.workspace1)
        DeleteWorkspace(self.workspace2)
        return super().tearDown()

    def generateTableWorkspace(self, workspaceName: str, columns: list):
        from mantid.simpleapi import CreateEmptyTableWorkspace

        CreateEmptyTableWorkspace(OutputWorkspace=workspaceName)
        for col in columns:
            mtd[workspaceName].addColumn("int", col)

        return workspaceName

    def test_buffer_missing_columns(self):
        BufferMissingColumnsAlgo(
            Workspace1=self.workspace1,
            Workspace2=self.workspace2,
        )

        # compare the columns of workspace2 to the expected columns
        # would need to have the same members and same order
        assert self.combinedColumns == mtd[self.workspace2].getColumnNames()

        # maintain workspace 1 is not touched
        assert self.workspace1Columns == mtd[self.workspace1].getColumnNames()
