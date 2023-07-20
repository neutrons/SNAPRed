import unittest.mock as mock

# Mock out of scope modules before importing DataExportService

with mock.patch.dict(
    "sys.modules",
    {
        "snapred.backend.log": mock.Mock(),
        "snapred.backend.log.logger": mock.Mock(),
        "snapred.backend.data.DataFactoryService": mock.Mock(),
    },
):
    from snapred.backend.recipe.VanadiumFocussedReductionRecipe import VanadiumFocussedReductionRecipe  # noqa: E402
    from snapred.backend.service.VanadiumFocussedReductionService import VanadiumFocussedReductionService  # noqa: E402

    # test export calibration

    def test_vanadiumFocussedReductionService():
        vanadiumService = VanadiumFocussedReductionService()
        VanadiumFocussedReductionRecipe.executeRecipe = mock.Mock()
        mock_runConfig = mock.MagicMock()
        mock_runConfig.runNumber = "1234"
        runs = [mock_runConfig]
        vanadiumService.vanadiumReduction(runs)
        VanadiumFocussedReductionRecipe.executeRecipe.assert_called()
