from PyQt5.QtWidgets import QComboBox, QPushButton 

from snapred.ui.view.BackendRequestView import BackendRequestView
from snapred.ui.widget.Toggle import Toggle


class InitializeCalibrationCheckView(BackendRequestView):

    def __init__(self, jsonForm, parent=None):
        super().__init__(jsonForm, "initializeCalibrationCheck/", parent=parent)
        self.runNumberField = self._labeledField("Run Number", jsonForm.getField("runNumber"))
        self.liteModeToggle = self._labeledField("Lite Mode", Toggle(parent=self))
        self.sampleDropdown = QComboBox()
        self.sampleDropdown.addItem("Select Sample")
        self.groupingFileDropdown = QComboBox()
        self.groupingFileDropdown.addItem("Select Grouping File")
        self.beginFlowButton = QPushButton("Check")
        self.layout.addWidget(self.runNumberField, 0, 0)
        self.layout.addWidget(self.liteModeToggle, 0, 1)
        self.layout.addWidget(self.sampleDropdown, 1, 0)
        self.layout.addWidget(self.groupingFileDropdown, 1, 1)
        self.layout.addWidget(self.beginFlowButton, 3, 0, 1, 2)

    def getRunNumber(self):
        return self.runNumberField.text()
