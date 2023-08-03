from PyQt5.QtGui import *
from PyQt5.QtWidgets import *
from PyQt5.QtCore import *

from snapred.ui.presenter.InitializeCalibrationPresenter import CalibrationCheck
from snapred.ui.widget.Toggle import Toggle

class CalibrationMenu(QDialog):
    def __init__(self, jsonForm, parent=None):
        super(CalibrationMenu, self).__init__(parent)
        self.setWindowTitle("Calibration Menu")

        layout = QGridLayout(self)

        self.runNumberField = QLineEdit()
        self.runNumberField.setPlaceholderText("Enter Run Number")
        layout.addWidget(self.runNumberField, 0, 0)

        sampleDropdown = QComboBox()
        sampleDropdown.addItem("Select Sample")
        groupingFileDropdown = QComboBox()
        groupingFileDropdown.addItem("Select Grouping File")

        layout.addWidget(sampleDropdown, 1, 0)
        layout.addWidget(groupingFileDropdown, 1, 1)

        self.beginFlowButton = QPushButton("Check")
        layout.addWidget(self.beginFlowButton, 2, 0, 1, 2)

        liteModeToggle = self._labeledField("Lite Mode", Toggle(parent=self))
        layout.addWidget(liteModeToggle, 0, 1)

        self.setLayout(layout)

        self.calibrationCheck = CalibrationCheck(self)

        self.beginFlowButton.clicked.connect(self.calibrationCheck.handleButtonClicked)

    def _labeledField(self, label, field):
        widget = QWidget()
        widget.setStyleSheet("background-color: #F5E9E2;")
        layout = QHBoxLayout(widget)
        label = QLabel(label)
        layout.addWidget(label)
        layout.addWidget(field)
        return widget
    
    def getRunNumber(self):
        return self.runNumberField.text()


class InitializeCalibrationCheckView(QWidget):

    def __init__(self, jsonForm, parent=None):
        super(InitializeCalibrationCheckView, self).__init__(parent)
        self.layout = QGridLayout()
        self.setLayout(self.layout)

        self.beginFlowButton = QPushButton("Check Calibration Initialization")
        self.layout.addWidget(self.beginFlowButton, 4, 0, 1, 2)

        self.calibrationCheck = CalibrationCheck(self)
        self.beginFlowButton.clicked.connect(lambda: self.launchCalibrationCheck(jsonForm))

    def launchCalibrationCheck(self, jsonForm):
        calibrationMenu = CalibrationMenu(jsonForm, parent=self)
        calibrationMenu.exec_()