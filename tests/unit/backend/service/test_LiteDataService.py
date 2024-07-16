import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock
from unittest.mock import Mock, patch

import pytest
from mantid.simpleapi import CloneWorkspace, CreateSingleValuedWorkspace
from snapred.backend.service.LiteDataService import LiteDataService
from snapred.meta.Config import Resource


class TestLiteDataService(unittest.TestCase):
    @patch("snapred.backend.service.LiteDataService.Recipe.executeRecipe")
    def test_reduceLiteData_calls_executeRecipe_with_correct_arguments(
        self,
        executeRecipe,
    ):
        executeRecipe.return_value = {}
        executeRecipe.side_effect = lambda **kwargs: CloneWorkspace(
            InputWorkspace=kwargs["InputWorkspace"],
            OutputWorkspace=kwargs["OutputWorkspace"],
        )

        inputWorkspace = "_test_liteservice_555"
        outputWorkspace = "_test_output_lite_"
        CreateSingleValuedWorkspace(OutputWorkspace=inputWorkspace)

        liteDataService = LiteDataService()
        liteDataService._ensureLiteDataMap = Mock(return_value="lite_map")
        with TemporaryDirectory(dir=Resource.getPath("outputs"), suffix="/") as tmpdir:
            outputPath = Path(tmpdir, "555.nxs.h5")
            assert not outputPath.exists()
            liteDataService.dataExportService.getFullLiteDataFilePath = mock.Mock(return_value=outputPath)

            liteDataService.reduceLiteData(inputWorkspace, outputWorkspace)
            assert outputPath.exists()

        assert executeRecipe.called_with(
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

        inputWorkspace = "_test_liteservice_555"
        outputWorkspace = "_test_output_lite_"

        with pytest.raises(RuntimeError) as e:
            liteDataService.reduceLiteData(inputWorkspace, outputWorkspace)
        assert "oops!" in str(e.value)
