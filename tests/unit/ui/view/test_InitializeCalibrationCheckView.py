from PyQt5.QtCore import Qt
from PyQt5.QtWidgets import QComboBox
from snapred.ui.view.InitializeCalibrationCheckView import CalibrationMenu


def test_run_number_field(qtbot):
    menu = CalibrationMenu(None)
    qtbot.addWidget(menu)

    assert menu.getRunNumber() == ""

    test_value = "1234"
    qtbot.keyClicks(menu.runNumberField, test_value)
    assert menu.getRunNumber() == test_value

    sample_dropdown = menu.findChild(QComboBox, "sampleDropdown")
    qtbot.mouseClick(sample_dropdown, Qt.LeftButton)
    sample_dropdown.setCurrentIndex(1)
    assert sample_dropdown.currentText() != "Select Sample"

    grouping_file_dropdown = menu.findChild(QComboBox, "groupingFileDropdown")
    qtbot.mouseClick(grouping_file_dropdown, Qt.LeftButton)
    grouping_file_dropdown.setCurrentIndex(1)
    assert grouping_file_dropdown.currentText() != "Select Grouping File"
