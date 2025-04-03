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

from snapred.backend.dao.Limit import Limit
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

    def test_bad_dspacing_range(self):
        pixel_group = self.sculleryBoy.prepPixelGroup()

        first_group_id = list(pixel_group.pixelGroupingParameters.keys())[0]
        group_params = pixel_group.pixelGroupingParameters[first_group_id]

        group_params.dResolution = Limit[float](minimum=0.1, maximum=2.0)

        ingredients = Recipe.Ingredients(pixelGroup=pixel_group, preserveEvents=True)
        recipe = Recipe()

        with (
            Config_override("constants.CropFactors.lowdSpacingCrop", 3.0),
            Config_override("constants.CropFactors.highdSpacingCrop", 0.0),
            pytest.raises(ValueError, match="Invalid d-spacing range detected: some dMax <= dMin."),
        ):
            recipe.chopIngredients(ingredients)
