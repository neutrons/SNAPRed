import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock
from unittest.mock import Mock, patch

import pytest
from mantid.simpleapi import CloneWorkspace, CreateSingleValuedWorkspace
from snapred.backend.dao.calibration import Calibration
from snapred.backend.dao.ingredients.PixelGroupingIngredients import PixelGroupingIngredients
from snapred.backend.dao.state.InstrumentState import InstrumentState
from snapred.backend.service.LiteDataService import LiteDataService
from snapred.meta.Config import Resource


class TestLiteDataService(unittest.TestCase):
    @patch("snapred.backend.service.LiteDataService.Recipe.executeRecipe")
    @patch("snapred.backend.service.LiteDataService.DataFactoryService.getCalibrationState")
    @patch("snapred.backend.service.LiteDataService.DataExportService.exportWorkspace")
    def test_reduceLiteData_calls_executeRecipe_with_correct_arguments(
        self,
        mock_exportWorkspace,  # noqa: ARG002
        mock_getCalibrationState,
        executeRecipe,
    ):
        executeRecipe.return_value = {}
        executeRecipe.side_effect = lambda **kwargs: CloneWorkspace(
            InputWorkspace=kwargs["InputWorkspace"],
            OutputWorkspace=kwargs["OutputWorkspace"],
        )

        # Mock the calibration state and ingredients
        mock_calibration = Mock(spec=Calibration)
        mock_instrument_state = Mock(spec=InstrumentState)
        mock_ingredients = Mock(spec=PixelGroupingIngredients)
        mock_ingredients.model_dump_json.return_value = '{"instrumentConfig": {}}'  # Add appropriate JSON content
        mock_instrument_state.instrumentConfig = mock_ingredients  # Mock the nested instrumentConfig
        mock_calibration.instrumentState = mock_instrument_state
        mock_getCalibrationState.return_value = mock_calibration

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

        executeRecipe.assert_called_with(
            InputWorkspace=inputWorkspace,
            OutputWorkspace=outputWorkspace,
            LiteDataMapWorkspace=liteDataService._ensureLiteDataMap.return_value,
            LiteInstrumentDefinitionFile=None,
            Ingredients=mock_ingredients.model_dump_json.return_value,
        )

    @patch("snapred.backend.recipe.GenericRecipe.GenericRecipe.executeRecipe")
    @patch("snapred.backend.service.LiteDataService.DataFactoryService.getCalibrationState")
    def test_reduceLiteData_fails(self, mock_getCalibrationState, mock_executeRecipe):
        mock_executeRecipe.return_value = {}
        mock_executeRecipe.side_effect = RuntimeError("oops!")

        # Mock the calibration state and ingredients
        mock_calibration = Mock(spec=Calibration)
        mock_instrument_state = Mock(spec=InstrumentState)
        mock_ingredients = Mock(spec=PixelGroupingIngredients)
        mock_ingredients.model_dump_json.return_value = '{"instrumentConfig": {}}'  # Add appropriate JSON content
        mock_instrument_state.instrumentConfig = mock_ingredients  # Mock the nested instrumentConfig
        mock_calibration.instrumentState = mock_instrument_state
        mock_getCalibrationState.return_value = mock_calibration

        from snapred.backend.service.LiteDataService import LiteDataService

        liteDataService = LiteDataService()
        liteDataService._ensureLiteDataMap = Mock(return_value="lite_map")

        inputWorkspace = "_test_liteservice_555"
        outputWorkspace = "_test_output_lite_"

        with pytest.raises(RuntimeError) as e:
            liteDataService.reduceLiteData(inputWorkspace, outputWorkspace)
        assert "oops!" in str(e.value)
