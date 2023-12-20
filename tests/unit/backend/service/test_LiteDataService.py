import unittest
from unittest.mock import patch

import pytest
from snapred.backend.dao.RunConfig import RunConfig


class TestLiteDataService(unittest.TestCase):
    @patch("snapred.backend.recipe.GenericRecipe.GenericRecipe.executeRecipe")
    def test_reduceLiteData_calls_executeRecipe_with_correct_arguments(
        self,
        mock_executeRecipe,
    ):
        mock_executeRecipe.return_value = {}

        from snapred.backend.service.LiteDataService import LiteDataService

        liteDataService = LiteDataService()

        inputWorkspace = "_test_liteservice_"
        liteMap = "_lite_map_"
        outputWorkspace = "_test_output_lite_"

        liteDataService.reduceLiteData(inputWorkspace, liteMap, outputWorkspace)

        assert mock_executeRecipe.called_with(
            InputWorkspace=inputWorkspace,
            LiteDataMapWorkspace=liteMap,
            OutputWorkspace=outputWorkspace,
        )

    @patch("snapred.backend.recipe.GenericRecipe.GenericRecipe.executeRecipe")
    def test_reduceLiteData_fails(self, mock_executeRecipe):
        mock_executeRecipe.return_value = {}
        mock_executeRecipe.side_effect = RuntimeError("oops!")

        from snapred.backend.service.LiteDataService import LiteDataService

        liteDataService = LiteDataService()

        inputWorkspace = "_test_liteservice_"
        liteMap = "_lite_map_"
        outputWorkspace = "_test_output_lite_"

        with pytest.raises(RuntimeError) as e:
            liteDataService.reduceLiteData(inputWorkspace, liteMap, outputWorkspace)
        assert "oops!" in str(e.value)
