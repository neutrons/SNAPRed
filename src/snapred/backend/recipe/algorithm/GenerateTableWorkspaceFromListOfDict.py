import json

from mantid.api import AlgorithmFactory, ITableWorkspaceProperty, PropertyMode, PythonAlgorithm
from mantid.kernel import Direction
from mantid.simpleapi import _create_algorithm_function

from snapred.backend.recipe.algorithm.MantidSnapper import MantidSnapper
from snapred.meta.Config import Config


class GenerateTableWorkspaceFromListOfDict(PythonAlgorithm):
    def category(self):
        return "SNAPRed Internal"

    def PyInit(self):
        # declare properties
        # shallow extraction
        self.declareProperty("ListOfDict", defaultValue="", direction=Direction.Input)
        self.declareProperty(
            ITableWorkspaceProperty("OutputWorkspace", "outputTable", Direction.Output, PropertyMode.Mandatory),
            doc="The table workspace created from the input",
        )
        self.setRethrows(True)
        self.mantidSnapper = MantidSnapper(self, self.__class__.__name__)

    def PyExec(self):
        listOfDict = json.loads(self.getProperty("ListOfDict").value)
        outputWorkspace = self.getPropertyValue("OutputWorkspace")
        ws = self.mantidSnapper.CreateEmptyTableWorkspace(
            "Initializing empty table workspace...",
            OutputWorkspace=outputWorkspace,
        )
        self.mantidSnapper.executeQueue()
        ws = self.mantidSnapper.mtd[outputWorkspace]
        if len(listOfDict) < 1:
            return

        firstRow = listOfDict[0]
        for key in firstRow.keys():
            _type = type(firstRow[key])
            if _type is float:
                ws.addColumn(type="double", name=key)
            else:
                ws.addColumn(type=_type.__name__, name=key)

        for row in listOfDict:
            ws.addRow([row[key] for key in row.keys()])

        self.setProperty("OutputWorkspace", outputWorkspace)
        return


# Register algorithm with Mantid
AlgorithmFactory.subscribe(GenerateTableWorkspaceFromListOfDict)
algo = GenerateTableWorkspaceFromListOfDict()
algo.initialize()
_create_algorithm_function(GenerateTableWorkspaceFromListOfDict.__name__, 1, algo)
