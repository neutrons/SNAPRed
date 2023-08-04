from snapred.ui.view.InitializeCalibrationCheckView import CalibrationMenu


def test_run_number_field():
    menu = CalibrationMenu(None)

    test_value = "1234"
    menu.runNumberField.setText(test_value)

    assert menu.getRunNumber() == test_value

    menu.close()
