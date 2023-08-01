from PyQt5.QtGui import *
from PyQt5.QtWidgets import *
from PyQt5.QtCore import *

from snapred.ui.widget.Toggle import Toggle

class InitializeCalibrationCheckView(QWidget):

    def __init__(self, jsonForm, parent=None):
        selection = "calibration/initializeCalibrationCheck"
        super(InitializeCalibrationCheckView, self).__init__(jsonForm, selection, parent=parent)
        runNumberField = self._labeledField("Run Number", jsonForm.getField("runNumber"))
        runNumberField.setObjectName("runNumberField")
        litemodeToggle = self._labeledField("Lite Mode", Toggle(parent=self))
        self.layout.addWidget(runNumberField, 0, 0)
        self.layout.addWidget(litemodeToggle, 0, 1)

        sampleDropdown = QComboBox()
        sampleDropdown.addItem("Select Sample")
        # todo: get samples from backend
        groupingFileDropdown = QComboBox()
        groupingFileDropdown.addItem("Select Grouping File")
        # todo get grouping files from backend
        self.layout.addWidget(sampleDropdown, 1, 0)
        self.layout.addWidget(groupingFileDropdown, 1, 1)
        self.beginFlowButton = QPushButton("Check")
        self.layout.addWidget(self.beginFlowButton, 3, 0, 1, 2)

    def getRunNumber(self):
        return self.findChild(QWidget, "runNumberField").text()