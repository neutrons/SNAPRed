import unittest
from unittest.mock import Mock, patch

import pytest


class TestLiteDataService(unittest.TestCase):
    @patch("snapred.backend.recipe.GenericRecipe.GenericRecipe.executeRecipe")
    def test_reduceLiteData_calls_executeRecipe_with_correct_arguments(
        self,
        mock_executeRecipe,
    ):
        mock_executeRecipe.return_value = {}

        from snapred.backend.service.LiteDataService import LiteDataService

        liteDataService = LiteDataService()
        liteDataService._ensureLiteDataMap = Mock(return_value="lite_map")

        inputWorkspace = "_test_liteservice_"
        outputWorkspace = "_test_output_lite_"
        runNumber = 555

        liteDataService.reduceLiteData(inputWorkspace, outputWorkspace, runNumber)

        assert mock_executeRecipe.called_with(
            InputWorkspace=inputWorkspace,
            OutputWorkspace=outputWorkspace,
            LiteDataMapWorkspace=liteDataService._ensureLiteDataMap.return_value,
        )

    @patch("snapred.backend.recipe.GenericRecipe.GenericRecipe.executeRecipe")
    def test_reduceLiteData_fails(self, mock_executeRecipe):
        mock_executeRecipe.return_value = {}
        mock_executeRecipe.side_effect = RuntimeError("oops!")

        from snapred.backend.service.LiteDataService import LiteDataService

        liteDataService = LiteDataService()
        liteDataService._ensureLiteDataMap = Mock(return_value="lite map")

        inputWorkspace = "_test_liteservice_"
        outputWorkspace = "_test_output_lite_"
        runNumber = 555

        with pytest.raises(RuntimeError) as e:
            liteDataService.reduceLiteData(inputWorkspace, outputWorkspace, runNumber)
        assert "oops!" in str(e.value)
