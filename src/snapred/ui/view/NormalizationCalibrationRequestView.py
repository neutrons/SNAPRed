from qtpy.QtWidgets import QComboBox

from snapred.ui.view.BackendRequestView import BackendRequestView


class NormalizationCalibrationRequestView(BackendRequestView):
    def __init__(self, jsonForm, samplePaths=[], groups=[], calibrantSamples=[], parent=None):
        selection = "calibration/reduction"
        super(NormalizationCalibrationRequestView, self).__init__(jsonForm, selection, parent=parent)
        self.runNumberField = self._labeledField("Run Number", jsonForm.getField("runNumber"))
        self.backgroundRunNumberField = self._labeledField(
            "Background Run Number", jsonForm.getField("backgroundRunNumber")
        )

        self.sampleDropDown = self._sampleDropDown("Sample", samplePaths)
        self.groupingFileDropDown = self._sampleDropDown("Grouping File", groups)
        self.calibrantSampleDropDown = self._sampleDropDown("Calibrant Sample", calibrantSamples)

        self.layout.addWidget(self.runNumberField, 0, 0)
        self.layout.addWidget(self.backgroundRunNumberField, 0, 1)
        self.layout.addWidget(self.sampleDropDown, 1, 0)
        self.layout.addWidget(self.groupingFileDropDown, 2, 0)
        self.layout.addWidget(self.calibrantSampleDropDown, 2, 1)

    def verify(self):
        if self.sampleDropDown.currentIndex() == 0:
            raise ValueError("Please select a sample")
        if self.calibrantSampleDropDown.currentIndex() == 0:
            raise ValueError("Please select a calibrant sample")
        if self.groupingFileDropDown.currentIndex() == 0:
            raise ValueError("Please select a grouping file")
        return True
