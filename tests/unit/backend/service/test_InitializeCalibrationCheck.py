import pytest
from unittest import mock
from PyQt5.QtWidgets import QMessageBox, QInputDialog

from snapred.backend.service.InitializeCalibrationServiceCheck import InitializeCalibrationCheck


def test_initialize_calibration_check():
    # Mock the QInputDialog.getText() method to return a state name
    with mock.patch.object(QInputDialog, 'getText', return_value=("Mocked State Name", True)):
        # Initialize the service
        service = InitializeCalibrationCheck()

        # Mock the necessary objects for the service methods
        run_config = mock.MagicMock()
        reduction_ingredients = mock.MagicMock()
        reduction_state = mock.MagicMock()
        state_config = mock.MagicMock()
        diffraction_calibrant = mock.MagicMock()
        state_config.diffractionCalibrant = diffraction_calibrant
        reduction_state.stateConfig = state_config
        reduction_ingredients.reductionState = reduction_state
        run_config.runNumber = "12345"
        run_config.maskFileName = "mock_mask"
        reduction_ingredients.runConfig = run_config

        # Mock the methods in the DataFactoryService and CalibrationService
        with mock.patch.object(service.dataFactory, 'getReductionIngredients', return_value=reduction_ingredients):
            with mock.patch.object(service.dataFactory, 'getStateConfig', return_value=state_config):
                with mock.patch.object(service.calibrationService, 'initializeState'):
                    with mock.patch.object(service.calibrationService, 'calculatePixelGroupingParameters') as mock_calculate:
                        # Set the mock return value for calculatePixelGroupingParameters
                        mock_calculate.return_value = {"param1": 1, "param2": 2}

                        # Call the method being tested
                        result = service.initializeCalibrationCheck([run_config])

                        # Assertions
                        assert result == {"param1": 1, "param2": 2}
                        mock_calculate.assert_called_once_with([run_config], reduction_ingredients.reductionState.stateConfig.focusGroups.definition)

        # Check that the state name was set correctly
        assert diffraction_calibrant.name == "Mocked State Name"

    # # Test the case where the QInputDialog is canceled or empty
    # with mock.patch.object(QInputDialog, 'getText', return_value=("", False)):
    #     service = InitializeCalibrationCheck()
    #     run_config = mock.MagicMock()
    #     reduction_ingredients = mock.MagicMock()
    #     reduction_state = mock.MagicMock()
    #     state_config = mock.MagicMock()
    #     diffraction_calibrant = mock.MagicMock()
    #     state_config.diffractionCalibrant = diffraction_calibrant
    #     reduction_state.stateConfig = state_config
    #     reduction_ingredients.reductionState = reduction_state
    #     run_config.runNumber = "12345"
    #     run_config.maskFileName = "mock_mask"
    #     reduction_ingredients.runConfig = run_config

    #     # Mock the methods in the DataFactoryService and CalibrationService
    #     with mock.patch.object(service.dataFactory, 'getReductionIngredients', return_value=reduction_ingredients):
    #         with mock.patch.object(service.dataFactory, 'getStateConfig', return_value=state_config):
    #             with mock.patch.object(service.calibrationService, 'initializeState'):
    #                 with mock.patch.object(service.calibrationService, 'calculatePixelGroupingParameters'):
    #                     result = service.initializeCalibrationCheck([run_config])
    #                     assert result is None

    # # Test the case where runs list is empty
    # with pytest.raises(ValueError, match="List is empty"):
    #     service = InitializeCalibrationCheck()
    #     service.initializeCalibrationCheck([])

    # # Test the exception case when calculatePixelGroupingParameters raises an exception
    # with mock.patch.object(QMessageBox, 'information') as mock_info:
    #     service = InitializeCalibrationCheck()
    #     run_config = mock.MagicMock()
    #     reduction_ingredients = mock.MagicMock()
    #     reduction_state = mock.MagicMock()
    #     state_config = mock.MagicMock()
    #     diffraction_calibrant = mock.MagicMock()
    #     state_config.diffractionCalibrant = diffraction_calibrant
    #     reduction_state.stateConfig = state_config
    #     reduction_ingredients.reductionState = reduction_state
    #     run_config.runNumber = "12345"
    #     run_config.maskFileName = "mock_mask"
    #     reduction_ingredients.runConfig = run_config

    #     # Mock the methods in the DataFactoryService and CalibrationService
    #     with mock.patch.object(service.dataFactory, 'getReductionIngredients', return_value=reduction_ingredients):
    #         with mock.patch.object(service.dataFactory, 'getStateConfig', return_value=state_config):
    #             with mock.patch.object(service.calibrationService, 'initializeState'):
    #                 with mock.patch.object(service.calibrationService, 'calculatePixelGroupingParameters') as mock_calculate:
    #                     # Set the mock return value for calculatePixelGroupingParameters
    #                     mock_calculate.side_effect = Exception("Unable to calculate pixel grouping parameters")
    #                     result = service.initializeCalibrationCheck([run_config])
    #                     assert result is None
    #                     mock_info.assert_called_once_with(None, "Ready to Calibrate", "All operations are complete. Ready to calibrate!")
