from mantid.api import PythonAlgorithm
from mantid.dataobjects import TableWorkspaceProperty
from mantid.kernel import Direction
from mantid.kernel import ULongLongPropertyWithValue as PointerProperty
from mantid.simpleapi import CreateEmptyTableWorkspace

from snapred.meta.pointer import access_pointer


class CreateTableWorkspace(PythonAlgorithm):
    def category(self):
        return "SNAPRed Internal"

    def PyInit(self):
        self.declareProperty(
            TableWorkspaceProperty("OutputWorkspace", "", Direction.Output),
            doc="The table workspace created from the input",
        )
        self.declareProperty(
            PointerProperty("Data", id(None), Direction.Input),
            doc="A dictionary with column names as keys, corresponding to a list for the column values",
        )
        self.setRethrows(True)

    def PyExec(self):
        data = access_pointer(self.getProperty("Data").value)
        colnames = list(data.keys())
        length = len(data[colnames[0]])
        for col in data.values():
            if len(col) != length:
                raise RuntimeError(f"Column mismatch: length {len(col)} vs {length}")

        outputWorkspace = self.getPropertyValue("OutputWorkspace")
        ws = CreateEmptyTableWorkspace(
            OutputWorkspace=outputWorkspace,
        )
        # add the columns
        for colname in colnames:
            coltype = type((data[colname][-1:] or [""])[0])
            if coltype is float:
                coltype = "double"
            else:
                coltype = coltype.__name__

            ws.addColumn(type=coltype, name=colname)
        # now add all the data in the columns
        for i in range(length):
            ws.addRow({colname: data[colname][i] for colname in colnames})

        self.setProperty("OutputWorkspace", ws)
