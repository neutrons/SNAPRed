import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock
from unittest.mock import Mock, patch

import pytest
from mantid.simpleapi import CloneWorkspace, CreateSingleValuedWorkspace
from util.Config_helpers import Config_override
from util.dao import DAOFactory

from snapred.backend.service.LiteDataService import LiteDataService
from snapred.meta.Config import Resource


class TestLiteDataService(unittest.TestCase):
    @patch("snapred.backend.service.LiteDataService.Recipe.executeRecipe")
    def test_createLiteData_calls_executeRecipe_with_correct_arguments(
        self,
        executeRecipe,
    ):
        executeRecipe.return_value = {}
        executeRecipe.side_effect = lambda **kwargs: (
            CloneWorkspace(
                InputWorkspace=kwargs["InputWorkspace"],
                OutputWorkspace=kwargs["OutputWorkspace"],
            ),
            0.04,
        )

        inputWorkspace = "_test_liteservice_555"
        outputWorkspace = "_test_output_lite_"
        CreateSingleValuedWorkspace(OutputWorkspace=inputWorkspace)

        liteDataService = LiteDataService()
        liteDataService._ensureLiteDataMap = Mock(return_value="lite_map")
        liteDataService.dataFactoryService.constructStateId = mock.Mock(
            return_value=(DAOFactory.real_state_id.hex, DAOFactory.real_detector_state)
        )
        liteDataService.sousChef.prepInstrumentState = Mock()
        liteDataService.sousChef.prepInstrumentState.return_value = Mock()
        liteDataService.sousChef.prepInstrumentState.return_value.model_dump_json.return_value = "{}"  # noqa: E501
        liteDataService.dataFactoryService = Mock()
        liteDataService.dataFactoryService.constructStateId = Mock(return_value=("state_id", "state"))

        with TemporaryDirectory(dir=Resource.getPath("outputs"), suffix="/") as tmpdir:
            outputPath = Path(tmpdir, "555.nxs.h5")
            assert not outputPath.exists()
            liteDataService.dataExportService.getFullLiteDataFilePath = mock.Mock(return_value=outputPath)

            liteDataService.createLiteData(inputWorkspace, outputWorkspace)
            assert outputPath.exists()

        executeRecipe.assert_called_with(
            InputWorkspace=inputWorkspace,
            OutputWorkspace=outputWorkspace,
            LiteDataMapWorkspace="lite_map",
            LiteInstrumentDefinitionFile=None,
            Ingredients="{}",
        )

        liteDataService.dataExportService = Mock()
        with Config_override("constants.LiteDataCreationAlgo.toggleCompressionTolerance", True):
            liteDataService.createLiteData(inputWorkspace, outputWorkspace)
            executeRecipe.assert_called_with(
                InputWorkspace=inputWorkspace,
                OutputWorkspace=outputWorkspace,
                LiteDataMapWorkspace="lite_map",
                LiteInstrumentDefinitionFile=None,
                Ingredients="{}",
                ToleranceOverride=-0.123,
            )

    @patch("snapred.backend.recipe.GenericRecipe.GenericRecipe.executeRecipe")
    def test_createLiteData_fails(self, mock_executeRecipe):
        mock_executeRecipe.return_value = {}
        mock_executeRecipe.side_effect = RuntimeError("oops!")

        liteDataService = LiteDataService()
        liteDataService._ensureLiteDataMap = Mock(return_value="lite map")
        liteDataService.dataFactoryService.constructStateId = mock.Mock(
            return_value=(DAOFactory.real_state_id.hex, DAOFactory.real_detector_state)
        )
        liteDataService.sousChef.prepInstrumentState = Mock()
        liteDataService.sousChef.prepInstrumentState.return_value = Mock()
        liteDataService.sousChef.prepInstrumentState.return_value.model_dump_json.return_value = "{}"  # noqa: E501
        liteDataService.dataFactoryService = Mock()
        liteDataService.dataFactoryService.constructStateId = Mock(return_value=("state_id", "state"))

        inputWorkspace = "_test_liteservice_555"
        outputWorkspace = "_test_output_lite_"

        with pytest.raises(RuntimeError) as e:
            liteDataService.createLiteData(inputWorkspace, outputWorkspace)
        assert "oops!" in str(e.value)
