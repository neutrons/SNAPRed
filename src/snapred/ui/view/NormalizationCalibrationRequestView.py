from qtpy.QtWidgets import QComboBox'

from snapred.ui.view.BackendRequestView import BackendRequestView


class NormalizationCalibrationRequestView(BackendRequestView):
    def __init__(self, jsonForm, calibrantSamples=[], parent=None):
        selection = "calibration/reduction"
        super(NormalizationCalibrationRequestView, self).__init__(jsonForm, selection, parent=parent)
        self.runNumberField = self._labeledField("Run Number", jsonForm.getField("runNumber"))
        self.emptyRunNumberField = self._labeledField("Empty Run Number", jsonForm.getField("emptyRunNumber"))
        self.fieldConvergnceThreshold = self._labeledField(
            "Convergence Threshold", jsonForm.getField("convergenceThreshold")
        )
        # might need to add inputs for absorption correciton here
        self.calibrantDropdown = self._sampleDropDown("Calibrant Sample", calibrantSamples)

        self.layout.addWidget(self.runNumberField, 0, 0)
        self.layout.addWidget(self.emptyRunNumberField, 0, 1)
        self.layout.addWidget(self.fieldConvergnceThreshold, 1, 0)
        self.layout.addWidget(self.calibrantDropdown, 1, 1)

    def verify(self):
        if self.calibrantDropdown.currentIndex() == 0:
            raise ValueError("Please select a sample")
        return True