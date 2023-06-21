import unittest.mock as mock

# Mock out of scope modules before importing DataExportService

with mock.patch.dict(
    "sys.modules",
    {
        "snapred.backend.recipe.FitMultiplePeaksRecipe": mock.Mock(),
        "snapred.backend.log": mock.Mock(),
        "snapred.backend.log.logger": mock.Mock(),
    },
):

    from snapred.backend.service.FitMultiplePeakService import FitMultiplePeaksService # noqa: E402
    from snapred.backend.dao.FitMultiplePeaksIngredients import FitMultiplePeaksIngredients # noqa: E402

    # test export calibration
    def test_fit_multiple_peaks():
        fitPeaksService = FitMultiplePeaksService()
        FitMultiplePeaksIngredients = mock.Mock()
        ingredients = FitMultiplePeaksIngredients()
        fitPeaksService.fit_multiple_peaks(ingredients)
        assert fitPeaksService.fit_multiple_peaks.called

