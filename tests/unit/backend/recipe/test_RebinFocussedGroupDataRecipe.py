import unittest

from mantid.api import MatrixWorkspace
from mantid.simpleapi import (
    CreateSampleWorkspace,
    DeleteWorkspaces,
    GroupDetectors,
    mtd,
)
from util.dao import DAOFactory
from util.SculleryBoy import SculleryBoy

from snapred.backend.recipe.RebinFocussedGroupDataRecipe import RebinFocussedGroupDataRecipe as Recipe

ThisRecipe: str = "snapred.backend.recipe.RebinFocussedGroupDataRecipe"


class TestRebinFocussedGroupDataRecipe(unittest.TestCase):
    sculleryBoy = SculleryBoy()

    def setUp(self):
        testCalibration = DAOFactory.calibrationRecord("57514", True, 1)
        self.pixelGroup = testCalibration.pixelGroups[0]
        self.pixelGroup
        self.ingredients = Recipe.Ingredients(pixelGroup=self.pixelGroup, preserveEvents=True)

        self.sampleWorkspace = "sampleWorkspace"
        CreateSampleWorkspace(
            OutputWorkspace=self.sampleWorkspace,
            BankPixelWidth=3,
        )
        GroupDetectors(
            InputWorkspace=self.sampleWorkspace,
            OutputWorkspace=self.sampleWorkspace,
            GroupingPattern="0-3,4-5,6-8,9-17",
        )

    def tearDown(self) -> None:
        DeleteWorkspaces(self.sampleWorkspace)
        return super().tearDown()

    def test_recipe(self):
        inputWs = mtd[self.sampleWorkspace]
        assert not inputWs.isRaggedWorkspace()
        recipe = Recipe()
        recipe.cook(self.ingredients, {"inputWorkspace": self.sampleWorkspace})

        outputWs = mtd[self.sampleWorkspace]
        assert isinstance(outputWs, MatrixWorkspace)
        assert outputWs.isRaggedWorkspace()
