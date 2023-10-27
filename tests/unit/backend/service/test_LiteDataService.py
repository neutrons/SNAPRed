import unittest
from unittest.mock import MagicMock, patch

from snapred.backend.dao.RunConfig import RunConfig


class TestLiteDataService(unittest.TestCase):
    @patch("snapred.backend.data.DataFactoryService.DataFactoryService", autospec=True)
    @patch("snapred.backend.recipe.GenericRecipe.LiteDataRecipe.executeRecipe")
    def test_reduceLiteData_calls_executeRecipe_with_correct_arguments(
        self, mock_executeRecipe, mock_dataFactoryService  # noqa: ARG002
    ):
        # Set the mock return value
        mock_executeRecipe.return_value = {}

        # Create a mock run configuration
        mock_runConfig = RunConfig(runNumber="12345")

        # Import the service to test
        from snapred.backend.service.LiteDataService import LiteDataService

        # Instantiate the service
        liteDataService = LiteDataService()

        # Call the method to test
        liteDataService.reduceLiteData([mock_runConfig])

        # Assert the method was called with the correct arguments
        mock_executeRecipe.assert_called_with(inputWorkspace="SNAP_12345.nxs", runNumber="12345")
