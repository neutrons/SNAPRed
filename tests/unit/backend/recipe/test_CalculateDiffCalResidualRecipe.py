import unittest

from mantid.api import WorkspaceGroup
from mantid.simpleapi import CreateSingleValuedWorkspace, mtd

from snapred.backend.recipe.algorithm.Utensils import Utensils
from snapred.backend.recipe.CalculateDiffCalResidualRecipe import CalculateDiffCalResidualRecipe


class CalculateDiffCalResidualRecipeTest(unittest.TestCase):
    def _make_groceries(self):
        inputWS = mtd.unique_name(prefix="test_resid")
        diagWS = mtd.unique_name(prefix="test_resid")
        fitDiagWS = mtd.unique_name(prefix="test_resid")
        outputWS = mtd.unique_name(prefix="test_resid")
        CreateSingleValuedWorkspace(OutputWorkspace=inputWS)
        ws = CreateSingleValuedWorkspace(OutputWorkspace=diagWS)
        fitDiag = WorkspaceGroup()
        fitDiag.addWorkspace(ws)
        mtd.add(fitDiagWS, fitDiag)
        return {
            "inputWorkspace": inputWS,
            "outputWorkspace": outputWS,
            "fitPeaksDiagnosticWorkspace": fitDiagWS,
        }

    def test_init(self):
        CalculateDiffCalResidualRecipe()

    def test_init_reuseUtensils(self):
        utensils = Utensils()
        utensils.PyInit()
        recipe = CalculateDiffCalResidualRecipe(utensils=utensils)
        assert recipe.mantidSnapper == utensils.mantidSnapper

    def test_validateInputs(self):
        groceries = self._make_groceries()
        recipe = CalculateDiffCalResidualRecipe()
        recipe.validateInputs(None, groceries)

    def test_chopIngredients(self):
        recipe = CalculateDiffCalResidualRecipe()
        recipe.chopIngredients()
