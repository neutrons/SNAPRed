from typing import Dict

from mantid.api import (
    AlgorithmFactory,
    ITableWorkspaceProperty,
    PropertyMode,
    PythonAlgorithm,
    mtd,
)
from mantid.kernel import Direction

from snapred.backend.recipe.algorithm.WashDishes import WashDishes


class ConjoinTableWorkspaces(PythonAlgorithm):
    """
    Easily combine two table workspaces
    """

    def category(self):
        return "SNAPRed Interna;"

    def PyInit(self):
        # declare properties
        self.declareProperty(
            ITableWorkspaceProperty("InputWorkspace1", "", Direction.InOut),
            doc="The first workspace, which will have the rows added to it",
        )
        self.declareProperty(
            ITableWorkspaceProperty("InputWorkspace2", "", Direction.Input),
            doc="The second workspace, to be conjoined to first",
        )
        self.declareProperty("AutoDelete", False, Direction.Input)
        self.setRethrows(True)

    def chopIngredients(self, ingredients=None):
        pass

    def unbagGroceries(self):
        self.wksp1 = self.getProperty("InputWorkspace1").value
        self.wksp2 = self.getProperty("InputWorkspace2").value

    def validateInputs(self) -> Dict[str, str]:
        errors = {}
        wksp1 = self.getProperty("InputWorkspace1").value
        wksp2 = self.getProperty("InputWorkspace2").value
        if wksp1.columnCount() != wksp2.columnCount():
            msg = "The tables have mismatched numbers of columns"
            errors["InputWorkspace1"] = msg
            errors["InputWorkspace2"] = msg
            return errors
        msgNames = ""
        for col1, col2 in zip(wksp1.getColumnNames(), wksp2.getColumnNames()):
            if col1 != col2:
                msgNames += f"Mismatched column names {col1} and {col2}\n"
        if msgNames != "":
            errors["InputWorkspace1"] = msgNames
            errors["InputWorkspace2"] = msgNames
        msgTypes = ""
        for type1, type2 in zip(wksp1.columnTypes(), wksp2.columnTypes()):
            if type1 != type2:
                msgTypes += f"Mismatched column types {type1} and {type2}"
        if msgTypes != "":
            errors["InputWorkspace1"] = msgTypes
            errors["InputWorkspace2"] = msgTypes
        return errors

    def PyExec(self) -> None:
        """
        This will easily combine two table workspaces.
        """
        self.chopIngredients()
        self.unbagGroceries()

        for i in range(self.wksp2.rowCount()):
            self.wksp1.addRow(self.wksp2.row(i))
        if self.getProperty("AutoDelete").value:
            WashDishes(self.wksp2)
        self.setProperty("InputWorkspace1", self.wksp1)


# Register algorithm with Mantid
AlgorithmFactory.subscribe(ConjoinTableWorkspaces)
