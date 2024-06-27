import time

from mantid.api import AlgorithmFactory, ITableWorkspaceProperty, PropertyMode, PythonAlgorithm
from mantid.kernel import Direction

from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper


class BufferMissingColumnsAlgo(PythonAlgorithm):
    """
    Buffer missing columns from one tableworkspace to another, then sort.
    """

    def category(self):
        return "SNAPRed Internal"

    def PyInit(self):
        # declare properties
        self.declareProperty(
            ITableWorkspaceProperty("Workspace1", "", Direction.Input, PropertyMode.Mandatory),
            doc="Workspace to buffer missing columns from",
        )
        self.declareProperty(
            ITableWorkspaceProperty("Workspace2", "", Direction.InOut, PropertyMode.Mandatory),
            doc="Workspace to buffer missing columns to",
        )
        self.mantidSnapper = MantidSnapper(self, __name__)

    def sortColumns(self, workspace):
        columns = zip(workspace.getColumnNames(), workspace.columnTypes())
        columns = sorted(columns, key=lambda x: x[0])

        # create a whole new table workspace, because we can't obviously change the order
        # of columns in an existing one for whatever reason
        tempTableName = f"{time.time()}_bufferMissingColumns_tempTable"
        tempTable = self.mantidSnapper.CreateEmptyTableWorkspace(
            "Make temp workspace to copy to", OutputWorkspace=tempTableName
        )
        self.mantidSnapper.executeQueue()
        tempTable = self.mantidSnapper.mtd[tempTableName]
        # init table
        for col, t in columns:
            tempTable.addColumn(type=t, name=col)

        for i in range(workspace.rowCount()):
            row = workspace.row(i)
            tempTable.addRow(row)

        # in order to maintain any grouped workspace the original workspace was a part of
        # we need to dump its original columns and rows and replace them with the new ones
        for col in workspace.getColumnNames():
            workspace.removeColumn(col)
        for col in columns:
            workspace.addColumn(type=col[1], name=col[0])
        for i in range(tempTable.rowCount()):
            row = tempTable.row(i)
            workspace.addRow(row)

        self.mantidSnapper.DeleteWorkspace("Deleting temp workspace", Workspace=tempTable)
        self.mantidSnapper.executeQueue()
        return workspace

    def PyExec(self) -> None:
        workspace1 = self.getProperty("Workspace1").value
        workspace2 = self.getProperty("Workspace2").value

        for col, t in zip(workspace1.getColumnNames(), workspace1.columnTypes()):
            if col not in workspace2.getColumnNames():
                workspace2.addColumn(type=t, name=col)

        workspace2 = self.sortColumns(workspace2)
        self.setProperty("Workspace2", workspace2)


# Register algorithm with Mantid
AlgorithmFactory.subscribe(BufferMissingColumnsAlgo)
