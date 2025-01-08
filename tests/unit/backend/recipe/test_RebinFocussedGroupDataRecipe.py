import unittest

import pytest
from mantid.api import MatrixWorkspace
from mantid.simpleapi import (
    CreateSampleWorkspace,
    DeleteWorkspaces,
    GroupDetectors,
    mtd,
)
from util.Config_helpers import Config_override
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

    def test_badChopIngredients(self):
        ingredients = Recipe.Ingredients(pixelGroup=self.sculleryBoy.prepPixelGroup())
        recipe = Recipe()
        with (
            Config_override("constants.CropFactors.lowdSpacingCrop", 500.0),
            Config_override("constants.CropFactors.highdSpacingCrop", 1000.0),
            pytest.raises(ValueError, match="d-spacing crop factors are too large"),
        ):
            recipe.chopIngredients(ingredients)
        #
        with (
            Config_override("constants.CropFactors.lowdSpacingCrop", -10.0),
            pytest.raises(ValueError, match="Low d-spacing crop factor must be positive"),
        ):
            recipe.chopIngredients(ingredients)
        #
        with (
            Config_override("constants.CropFactors.highdSpacingCrop", -10.0),
            pytest.raises(ValueError, match="High d-spacing crop factor must be positive"),
        ):
            recipe.chopIngredients(ingredients)
