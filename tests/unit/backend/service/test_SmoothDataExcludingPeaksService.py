import unittest.mock as mock

with mock.patch.dict(
    "sys.modules",
    {
        "snapred.backend.log": mock.Mock(),
        "snapred.backend.log.logger": mock.Mock(),
    },
):
    from snapred.backend.recipe.SmoothDataExcludingPeaksRecipe import SmoothDataExcludingPeaksRecipe
    from snapred.backend.service.SmoothDataExcludingPeaksService import SmoothDataExcludingPeaksService

    def test_smooth_data_excluding_peaks():
        smoothdataService = SmoothDataExcludingPeaksService()
        SmoothDataExcludingPeaksRecipe.executeRecipe = mock.Mock()
        smoothdataIngredients = mock.Mock()
        smoothdataService.smooth_data_excluding_peaks(smoothdataIngredients)
        SmoothDataExcludingPeaksRecipe.executeRecipe.assert_called()
