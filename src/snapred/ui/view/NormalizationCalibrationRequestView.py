from qtpy.QtWidgets import QComboBox

from snapred.meta.decorators.Resettable import Resettable
from snapred.ui.view.BackendRequestView import BackendRequestView
from snapred.ui.widget.Toggle import Toggle


@Resettable
class NormalizationCalibrationRequestView(BackendRequestView):
    def __init__(self, jsonForm, samplePaths=[], groups=[], parent=None):
        selection = "calibration/diffractionCalibration"
        super(NormalizationCalibrationRequestView, self).__init__(jsonForm, selection, parent=parent)
        self.runNumberField = self._labeledField("Run Number:", jsonForm.getField("runNumber"))
        self.litemodeToggle = self._labeledField("Lite Mode", Toggle(parent=self, state=True))
        self.backgroundRunNumberField = self._labeledField(
            "Background Run Number:", jsonForm.getField("backgroundRunNumber")
        )

        self.litemodeToggle.setEnabled(False)
        self.layout.addWidget(self.runNumberField, 0, 0)
        self.layout.addWidget(self.litemodeToggle, 0, 1)
        self.layout.addWidget(self.backgroundRunNumberField, 1, 0)

        self.sampleDropDown = QComboBox()
        self.sampleDropDown.addItem("Select Sample")
        self.sampleDropDown.addItems(samplePaths)
        self.sampleDropDown.model().item(0).setEnabled(False)

        self.groupingFileDropDown = QComboBox()
        self.groupingFileDropDown.addItem("Select Grouping File")
        self.groupingFileDropDown.addItems(groups)
        self.groupingFileDropDown.model().item(0).setEnabled(False)

        self.layout.addWidget(self.sampleDropDown, 2, 0)
        self.layout.addWidget(self.groupingFileDropDown, 2, 1)

    def verify(self):
        if self.sampleDropDown.currentIndex() == 0:
            raise ValueError("Please select a sample")
        if self.groupingFileDropDown.currentIndex() == 0:
            raise ValueError("Please select a grouping file")
        if self.runNumberField.text() == "":
            raise ValueError("Please enter a run number")
        if self.backgroundRunNumberField.text() == "":
            raise ValueError("Please enter a background run number")
        return True
