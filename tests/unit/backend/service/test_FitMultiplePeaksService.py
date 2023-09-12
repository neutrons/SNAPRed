import unittest.mock as mock

# Mock out of scope modules before importing DataExportService

with mock.patch.dict(
    "sys.modules",
    {
        "snapred.backend.log": mock.Mock(),
        "snapred.backend.log.logger": mock.Mock(),
    },
):
    from snapred.backend.recipe.GenericRecipe import FitMultiplePeaksRecipe  # noqa: E402
    from snapred.backend.service.FitMultiplePeakService import FitMultiplePeakService  # noqa: E402

    # test export calibration
    def test_fit_multiple_peaks():
        fitPeaksService = FitMultiplePeakService()
        FitMultiplePeaksRecipe.executeRecipe = mock.Mock()
        FitMultiplePeaksIngredients = mock.Mock()
        ingredients = FitMultiplePeaksIngredients()
        fitPeaksService.fit_multiple_peaks(ingredients)
        FitMultiplePeaksRecipe.executeRecipe.assert_called()
