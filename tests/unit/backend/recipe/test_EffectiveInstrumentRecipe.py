# Test-specific imports go last.
from unittest import mock

import numpy as np
import pytest
from mantid.simpleapi import CreateSampleWorkspace, DeleteWorkspace, mtd
from util.SculleryBoy import SculleryBoy

from snapred.backend.dao.ingredients import EffectiveInstrumentIngredients as Ingredients
from snapred.backend.dao.state.FocusGroup import FocusGroup
from snapred.backend.dao.state.PixelGroup import PixelGroup
from snapred.backend.error.AlgorithmException import AlgorithmException
from snapred.backend.recipe.algorithm.Utensils import Utensils
from snapred.backend.recipe.EffectiveInstrumentRecipe import EffectiveInstrumentRecipe
from snapred.meta.Config import Resource


class TestEffectiveInstrumentRecipe:
    fakeInstrumentFilePath = Resource.getPath("inputs/testInstrument/fakeSNAP_Definition.xml")
    sculleryBoy = SculleryBoy()

    @pytest.fixture(autouse=True)
    def _setup(self):
        self.inputWS = mtd.unique_name()
        CreateSampleWorkspace(OutputWorkspace=self.inputWS, NumBanks=1, BankPixelWidth=2)

        self.ingredients = mock.Mock(
            spec=Ingredients,
            unmaskedPixelGroup=mock.Mock(
                spec=PixelGroup,
                groupIDs=[1, 2, 3, 4],
                L2=[10.0, 20.0, 30.0, 40.0],
                twoTheta=[0.0, 20.0, 30.0, 40.0],
                azimuth=[10.0, 15.0, 20.0, 25.0],
                focusGroup=FocusGroup(name="a_grouping", definition="a/grouping/path"),
            ),
        )
        self.ingredients1 = mock.Mock(
            spec=Ingredients,
            unmaskedPixelGroup=mock.Mock(
                spec=PixelGroup,
                groupIDs=[1, 2, 3, 4],
                L2=[10.0, 20.0, 30.0, 90.0],
                twoTheta=[0.0, 20.0, 40.0, 45.0],
                azimuth=[10.0, 15.0, 25.0, 30.0],
                focusGroup=FocusGroup(name="another_grouping", definition="another/grouping/path"),
            ),
        )
        self.ingredientss = [self.ingredients, self.ingredients1]

        yield

        # teardown follows ...
        DeleteWorkspace(self.inputWS)

    def test_allGroceryKeys(self):
        assert {"inputWorkspace", "outputWorkspace"} == EffectiveInstrumentRecipe().allGroceryKeys()
        assert {"inputWorkspace"} == EffectiveInstrumentRecipe().mandatoryInputWorkspaces()

    def test_chopIngredients(self):
        recipe = EffectiveInstrumentRecipe()
        ingredients = self.ingredients
        recipe.chopIngredients(ingredients)
        assert recipe.unmaskedPixelGroup == ingredients.unmaskedPixelGroup

    def test_unbagGroceries(self):
        recipe = EffectiveInstrumentRecipe()
        groceries = {"inputWorkspace": self.inputWS, "outputWorkspace": mock.Mock()}
        recipe.unbagGroceries(groceries)
        assert recipe.inputWS == groceries["inputWorkspace"]
        assert recipe.outputWS == groceries["outputWorkspace"]

    def test_unbagGroceries_output_default(self):
        recipe = EffectiveInstrumentRecipe()
        groceries = {"inputWorkspace": self.inputWS}
        recipe.unbagGroceries(groceries)
        assert recipe.inputWS == groceries["inputWorkspace"]
        assert recipe.outputWS == groceries["inputWorkspace"]

    def test_queueAlgos(self):
        recipe = EffectiveInstrumentRecipe()
        ingredients = self.ingredients
        groceries = {"inputWorkspace": self.inputWS, "outputWorkspace": mock.Mock()}
        recipe.prep(ingredients, groceries)
        recipe.queueAlgos()

        queuedAlgos = recipe.mantidSnapper._algorithmQueue

        cloneWorkspaceTuple = queuedAlgos[0]
        assert cloneWorkspaceTuple[0] == "CloneWorkspace"
        assert cloneWorkspaceTuple[2]["InputWorkspace"] == groceries["inputWorkspace"]
        assert cloneWorkspaceTuple[2]["OutputWorkspace"] == groceries["outputWorkspace"]

        editInstrumentGeometryTuple = queuedAlgos[1]
        assert editInstrumentGeometryTuple[0] == "EditInstrumentGeometry"
        assert editInstrumentGeometryTuple[2]["Workspace"] == groceries["outputWorkspace"]

    def test_queueAlgos_default(self):
        recipe = EffectiveInstrumentRecipe()
        ingredients = self.ingredients
        groceries = {"inputWorkspace": self.inputWS}
        recipe.prep(ingredients, groceries)
        recipe.queueAlgos()

        queuedAlgos = recipe.mantidSnapper._algorithmQueue

        editInstrumentGeometryTuple = queuedAlgos[0]
        assert editInstrumentGeometryTuple[0] == "EditInstrumentGeometry"
        assert editInstrumentGeometryTuple[2]["Workspace"] == groceries["inputWorkspace"]

    def test_cook(self):
        utensils = Utensils()
        mockSnapper = mock.Mock()
        mockSnapper.mtd = mtd
        utensils.mantidSnapper = mockSnapper
        recipe = EffectiveInstrumentRecipe(utensils=utensils)
        ingredients = self.ingredients
        groceries = {"inputWorkspace": self.inputWS, "outputWorkspace": mock.Mock()}

        output = recipe.cook(ingredients, groceries)

        assert output == groceries["outputWorkspace"]

        assert mockSnapper.executeQueue.called
        mockSnapper.CloneWorkspace.assert_called_once_with(
            "Clone workspace for reduced instrument",
            OutputWorkspace=groceries["outputWorkspace"],
            InputWorkspace=groceries["inputWorkspace"],
        )
        mockSnapper.EditInstrumentGeometry.assert_called_once_with(
            f"Editing instrument geometry for grouping '{ingredients.unmaskedPixelGroup.focusGroup.name}'",
            Workspace=groceries["outputWorkspace"],
            L2=ingredients.unmaskedPixelGroup.L2,
            # this form compares as `np.float64`, which is distinct from `float`:
            Polar=[np.rad2deg(p) for p in ingredients.unmaskedPixelGroup.twoTheta],
            Azimuthal=[np.rad2deg(a) for a in ingredients.unmaskedPixelGroup.azimuth],
            InstrumentName=f"SNAP_{ingredients.unmaskedPixelGroup.focusGroup.name}",
        )

    def test_cook_default(self):
        utensils = Utensils()
        mockSnapper = mock.Mock()
        mockSnapper.mtd = mtd
        utensils.mantidSnapper = mockSnapper
        recipe = EffectiveInstrumentRecipe(utensils=utensils)
        ingredients = self.ingredients
        groceries = {"inputWorkspace": self.inputWS}

        output = recipe.cook(ingredients, groceries)

        assert output == groceries["inputWorkspace"]

        assert mockSnapper.executeQueue.called
        mockSnapper.CloneWorkspace.assert_not_called()
        mockSnapper.EditInstrumentGeometry.assert_called_once_with(
            f"Editing instrument geometry for grouping '{ingredients.unmaskedPixelGroup.focusGroup.name}'",
            Workspace=groceries["inputWorkspace"],
            L2=ingredients.unmaskedPixelGroup.L2,
            # this form compares as `np.float64`, which is distinct from `float`:
            Polar=[np.rad2deg(p) for p in ingredients.unmaskedPixelGroup.twoTheta],
            Azimuthal=[np.rad2deg(a) for a in ingredients.unmaskedPixelGroup.azimuth],
            InstrumentName=f"SNAP_{ingredients.unmaskedPixelGroup.focusGroup.name}",
        )

    def test_cook_fail(self):
        # Test that `AlgorithmException` is routed to `RuntimeError`.
        utensils = Utensils()
        mockSnapper = mock.Mock()
        mockSnapper.mtd = mtd
        mockSnapper.executeQueue = mock.Mock(side_effect=AlgorithmException("EditInstrumentGeometry"))
        utensils.mantidSnapper = mockSnapper
        recipe = EffectiveInstrumentRecipe(utensils=utensils)
        ingredients = self.ingredients
        groceries = {"inputWorkspace": self.inputWS}

        with pytest.raises(RuntimeError, match=r".*EditInstrumentGeometry.*"):
            recipe.cook(ingredients, groceries)

    def test_cater(self):
        untensils = Utensils()
        mockSnapper = mock.Mock()
        mockSnapper.mtd = mtd
        untensils.mantidSnapper = mockSnapper
        recipe = EffectiveInstrumentRecipe(utensils=untensils)
        ingredientss = self.ingredientss

        groceriess = [{"inputWorkspace": self.inputWS}, {"inputWorkspace": self.inputWS}]

        recipe.cater(zip(ingredientss, groceriess))

        assert mockSnapper.EditInstrumentGeometry.call_count == 2
        mockSnapper.EditInstrumentGeometry.assert_any_call(
            f"Editing instrument geometry for grouping '{ingredientss[0].unmaskedPixelGroup.focusGroup.name}'",
            Workspace=groceriess[0]["inputWorkspace"],
            L2=ingredientss[0].unmaskedPixelGroup.L2,
            # this form compares as `np.float64`, which is distinct from `float`:
            Polar=[np.rad2deg(p) for p in ingredientss[0].unmaskedPixelGroup.twoTheta],
            Azimuthal=[np.rad2deg(a) for a in ingredientss[0].unmaskedPixelGroup.azimuth],
            InstrumentName=f"SNAP_{ingredientss[0].unmaskedPixelGroup.focusGroup.name}",
        )
        mockSnapper.EditInstrumentGeometry.assert_any_call(
            f"Editing instrument geometry for grouping '{ingredientss[1].unmaskedPixelGroup.focusGroup.name}'",
            Workspace=groceriess[1]["inputWorkspace"],
            L2=ingredientss[1].unmaskedPixelGroup.L2,
            # this form compares as `np.float64`, which is distinct from `float`:
            Polar=[np.rad2deg(p) for p in ingredientss[1].unmaskedPixelGroup.twoTheta],
            Azimuthal=[np.rad2deg(a) for a in ingredientss[1].unmaskedPixelGroup.azimuth],
            InstrumentName=f"SNAP_{ingredientss[1].unmaskedPixelGroup.focusGroup.name}",
        )
