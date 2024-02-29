from PyQt5.QtCore import Qt
from PyQt5.QtWidgets import QComboBox
from snapred.ui.view.InitializeStateCheckView import CalibrationMenu


def test_run_number_field(qtbot):
    menu = CalibrationMenu(None)
    qtbot.addWidget(menu)

    assert menu.getRunNumber() == ""

    test_value = "1234"
    qtbot.keyClicks(menu.runNumberField, test_value)
    assert menu.getRunNumber() == test_value
