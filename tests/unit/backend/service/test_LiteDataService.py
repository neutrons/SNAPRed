from pathlib import Path
from tempfile import TemporaryDirectory

from mantid.simpleapi import CloneWorkspace, CreateSingleValuedWorkspace

from snapred.backend.dao.ingredients.LiteDataCreationIngredients import LiteDataCreationIngredients
from snapred.backend.dao.state.InstrumentState import InstrumentState
from snapred.backend.service.LiteDataService import LiteDataService
from snapred.meta.Config import Resource

import unittest
from unittest import mock
import pytest
from util.Config_helpers import Config_override
from util.dao import DAOFactory


class TestLiteDataService(unittest.TestCase):
    @mock.patch("snapred.backend.service.LiteDataService.LiteDataRecipe.executeRecipe")
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
        liteDataService._ensureLiteDataMap = mock.Mock(return_value="lite_map")
        liteDataService.dataFactoryService.constructStateId = mock.Mock(
            return_value=(DAOFactory.real_state_id.hex, DAOFactory.real_detector_state)
        )
        liteDataService.sousChef.prepInstrumentState = mock.Mock()
        liteDataService.sousChef.prepInstrumentState.return_value = DAOFactory.default_instrument_state.model_copy()
        liteDataService.dataFactoryService = mock.Mock()
        liteDataService.dataFactoryService.constructStateId = mock.Mock(return_value=(DAOFactory.magical_state_id.hex, None))
        expectedIngredients = LiteDataCreationIngredients(
            instrumentState=liteDataService.sousChef.prepInstrumentState.return_value
        )

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
            Ingredients=expectedIngredients.model_dump_json()
        )

        liteDataService.dataExportService = mock.Mock()
        with Config_override("constants.LiteDataCreationAlgo.toggleCompressionTolerance", True):
            expectedIngredients = LiteDataCreationIngredients(
                instrumentState=liteDataService.sousChef.prepInstrumentState.return_value,
                toleranceOverride=-0.123
            )
            
            liteDataService.createLiteData(inputWorkspace, outputWorkspace)
            executeRecipe.assert_called_with(
                InputWorkspace=inputWorkspace,
                OutputWorkspace=outputWorkspace,
                LiteDataMapWorkspace="lite_map",
                LiteInstrumentDefinitionFile=None,
                Ingredients=expectedIngredients.model_dump_json()
            )

    @mock.patch("snapred.backend.recipe.GenericRecipe.GenericRecipe.executeRecipe")
    def test_createLiteData_fails(self, mock_executeRecipe):
        mock_executeRecipe.return_value = {}
        mock_executeRecipe.side_effect = RuntimeError("oops!")

        liteDataService = LiteDataService()
        liteDataService._ensureLiteDataMap = mock.Mock(return_value="lite map")
        liteDataService.dataFactoryService.constructStateId = mock.Mock(
            return_value=(DAOFactory.real_state_id.hex, DAOFactory.real_detector_state)
        )
        liteDataService.sousChef.prepInstrumentState = mock.Mock()
        liteDataService.sousChef.prepInstrumentState.return_value = DAOFactory.default_instrument_state.model_copy()
        liteDataService.dataFactoryService = mock.Mock()
        liteDataService.dataFactoryService.constructStateId = mock.Mock(return_value=(DAOFactory.magical_state_id.hex, None))

        inputWorkspace = "_test_liteservice_555"
        outputWorkspace = "_test_output_lite_"

        with pytest.raises(RuntimeError) as e:
            liteDataService.createLiteData(inputWorkspace, outputWorkspace)
        assert "oops!" in str(e.value)
