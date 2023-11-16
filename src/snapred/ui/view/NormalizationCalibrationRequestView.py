from qtpy.QtWidgets import QComboBox

from snapred.ui.view.BackendRequestView import BackendRequestView
from snapred.ui.widget.Toggle import Toggle


class NormalizationCalibrationRequestView(BackendRequestView):
    def __init__(self, jsonForm, samplePaths=[], groups=[], calibrantSamples=[], parent=None):
        selection = "calibration/reduction"
        super(NormalizationCalibrationRequestView, self).__init__(jsonForm, selection, parent=parent)
        self.runNumberField = self._labeledField("Run Number:", jsonForm.getField("runNumber"))
        self.litemodeToggle = self._labeledField("Lite Mode", Toggle(parent=self, state=True))
        self.backgroundRunNumberField = self._labeledField(
            "Background Run Number:", jsonForm.getField("backgroundRunNumber")
        )
        self.smoothingParameterField = self._labeledField(
            "Smoothing Parameter:", jsonForm.getField("smoothingParameter")
        )

        self.litemodeToggle.setEnabled(False)
        self.layout.addWidget(self.runNumberField, 0, 0)
        self.layout.addWidget(self.litemodeToggle, 0, 1)
        self.layout.addWidget(self.backgroundRunNumberField, 1, 0)
        self.layout.addWidget(self.smoothingParameterField, 1, 1)

        self.sampleDropDown = QComboBox()
        self.sampleDropDown.addItem("Select Sample")
        self.sampleDropDown.addItems(samplePaths)
        self.sampleDropDown.model().item(0).setEnabled(False)

        self.groupingFileDropDown = QComboBox()
        self.groupingFileDropDown.addItem("Select Grouping File")
        self.groupingFileDropDown.addItems(groups)
        self.groupingFileDropDown.model().item(0).setEnabled(False)

        self.calibrantSampleDropDown = QComboBox()
        self.calibrantSampleDropDown.addItem("Select Calibrant Sample")
        self.calibrantSampleDropDown.addItems(calibrantSamples)
        self.calibrantSampleDropDown.model().item(0).setEnabled(False)

        self.layout.addWidget(self.sampleDropDown, 2, 0)
        self.layout.addWidget(self.groupingFileDropDown, 2, 1)
        self.layout.addWidget(self.calibrantSampleDropDown, 3, 0)

    def verify(self):
        if self.sampleDropDown.currentIndex() == 0:
            raise ValueError("Please select a sample")
        if self.calibrantSampleDropDown.currentIndex() == 0:
            raise ValueError("Please select a calibrant sample")
        if self.groupingFileDropDown.currentIndex() == 0:
            raise ValueError("Please select a grouping file")
        return True
