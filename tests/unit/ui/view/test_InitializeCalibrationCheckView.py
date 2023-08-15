import sys

from PyQt5.QtCore import *
from PyQt5.QtGui import *
from PyQt5.QtWidgets import QApplication, QComboBox
from snapred.ui.view.InitializeCalibrationCheckView import CalibrationMenu

app = QApplication(sys.argv)


def test_run_number_field():
    menu = CalibrationMenu(None)
    menu.show()

    assert menu.getRunNumber() == ""

    test_value = "1234"
    menu.runNumberField.setText(test_value)
    assert menu.getRunNumber() == test_value

    test_value = "5678"
    menu.runNumberField.setText(test_value)
    assert menu.getRunNumber() == test_value

    sample_dropdown = menu.findChild(QComboBox, "sampleDropdown")
    sample_dropdown.setCurrentIndex(1)
    assert sample_dropdown.currentText() != "Select Sample"

    grouping_file_dropdown = menu.findChild(QComboBox, "groupingFileDropdown")
    grouping_file_dropdown.setCurrentIndex(1)
    assert grouping_file_dropdown.currentText() != "Select Grouping File"

    menu.close()
