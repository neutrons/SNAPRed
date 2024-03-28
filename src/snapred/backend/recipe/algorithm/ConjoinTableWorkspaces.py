from typing import Dict
from mantid.api import (
    AlgorithmFactory,
    ITableWorkspaceProperty,
    PythonAlgorithm,
)
from mantid.kernel import Direction
class ConjoinTableWorkspaces(PythonAlgorithm):
    """
    Easily combine two table workspaces
    """

    def category(self):
        return "SNAPRed Interna;"

    def PyInit(self):
        # declare properties
        self.declareProperty(
            ITableWorkspaceProperty("InputWorkspace1", "", Direction.Input),
            doc="The first workspace, which will have the rows added to it",
        )
        self.declareProperty(
            ITableWorkspaceProperty("InputWorkspace2", "", Direction.Input),
            doc="The second workspace, to be conjoined to first",
        )
        self.setRethrows(True)

    def chopIngredients(self, ingredients=None):
        pass

    def unbagGroceroes(self):
        self.wksp1 = self.getProperty("InputWorkspace1")
        self.wksp2 = self.getProperty("InputWorkspace2")

    def validateInputs(self) -> Dict[str, str]:
        errors = {}
        wksp1 = self.getProperty("InputWorkspace1")
        wksp2 = self.getProperty("InputWorkspace2")
        if wksp1.columnCount() != wksp2.columnCount():
            errors["Incompatible Sizes"] ="The tables have mismatched numbers of columns"
        for col1, col2 in zip(wksp1.getColumnNames(), wksp2.getColumnNames()):
            if col1 != col2:
                errors[f"{col1}-{col2}"] = "The tables have mismatched column names"
        for type1, type2 in zip(wksp1.columnTypes(), wksp2.columnTypes()):
            if type1 != type2:
                errors[f"{type1-type2}"] = "The tables have mismatched column types"
        return errors
        
    def PyExec(self) -> None:
        """
        This will easily combine two table workspaces.
        """

        for i in range(self.wksp2.rowCount()):
            self.wksp1.addRow(self.wksp2.row(i))
        self.setProperty("InputWorkspace1", self.wksp1)

# Register algorithm with Mantid
AlgorithmFactory.subscribe(ConjoinTableWorkspaces)
