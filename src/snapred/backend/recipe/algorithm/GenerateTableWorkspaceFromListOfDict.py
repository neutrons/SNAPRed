import json

from mantid.api import AlgorithmFactory, PythonAlgorithm
from mantid.kernel import Direction

from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper
from snapred.meta.Config import Config


class GenerateTableWorkspaceFromListOfDict(PythonAlgorithm):
    def PyInit(self):
        # declare properties
        # shallow extraction
        self.declareProperty("ListOfDict", defaultValue="", direction=Direction.Input)
        self.declareProperty("OutputWorkspace", defaultValue="outputTable", direction=Direction.Output)
        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, self.__class__.__name__)

    def PyExec(self):
        listOfDict = json.loads(self.getProperty("ListOfDict").value)
        outputWorkspace = self.getProperty("OutputWorkspace").value
        self.mantidSnapper.CreateEmptyTableWorkspace(
            "Initializing empty table workspace...", OutputWorkspace=outputWorkspace
        )
        self.mantidSnapper.executeQueue()
        ws = self.mantidSnapper.mtd[outputWorkspace]
        if len(listOfDict) < 1:
            return

        firstRow = listOfDict[0]
        for key in firstRow.keys():
            _type = type(firstRow[key])
            if _type in [float, int]:
                ws.addColumn(type="float", name=key)

        for row in listOfDict:
            ws.addRow([row[key] for key in row.keys()])

        self.setProperty("OutputWorkspace", outputWorkspace)
        return


# Register algorithm with Mantid
AlgorithmFactory.subscribe(GenerateTableWorkspaceFromListOfDict)
