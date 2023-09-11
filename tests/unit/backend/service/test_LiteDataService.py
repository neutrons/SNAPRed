import unittest
from unittest.mock import patch

from snapred.backend.dao.RunConfig import RunConfig


class TestLiteDataService(unittest.TestCase):
    @patch("snapred.backend.data.DataFactoryService.DataFactoryService", autospec=True)
    @patch("snapred.backend.recipe.GenericRecipe.GenericRecipe.executeRecipe")
    def test_reduceLiteData_calls_executeRecipe_with_correct_arguments(
        self, mock_executeRecipe, mock_dataFactoryService  # noqa: ARG002
    ):
        mock_executeRecipe.return_value = {}

        mock_runConfig = RunConfig(runNumber="12345")

        from snapred.backend.service.LiteDataService import LiteDataService

        liteDataService = LiteDataService()

        liteDataService.reduceLiteData([mock_runConfig])

        mock_executeRecipe.assert_called_with(inputWorkspace="SNAP_12345.nxs")
