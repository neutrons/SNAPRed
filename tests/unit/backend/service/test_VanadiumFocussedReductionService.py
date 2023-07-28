import unittest.mock as mock

# Mock out of scope modules

with mock.patch.dict(
    "sys.modules",
    {
        "snapred.backend.log": mock.Mock(),
        "snapred.backend.log.logger": mock.Mock(),
        "snapred.backend.data.DataFactoryService": mock.Mock(),
    },
):
    from snapred.backend.recipe.GenericRecipe import VanadiumFocussedReductionRecipe  # noqa: E402
    from snapred.backend.service.VanadiumFocussedReductionService import VanadiumFocussedReductionService  # noqa: E402

    def test_vanadiumFocussedReductionService():
        vanadiumService = VanadiumFocussedReductionService()
        VanadiumFocussedReductionRecipe.executeRecipe = mock.Mock()
        mock_ingredients = mock.Mock()
        mock_ingredients.run = mock.Mock()
        mock_ingredients.smoothIngredients = mock.Mock()
        vanadiumService.dataFactoryService.getReductionIngredients = mock.Mock()

        vanadiumService.vanadiumReduction(mock_ingredients)
        VanadiumFocussedReductionRecipe.executeRecipe.assert_called()
