from qtpy.QtWidgets import QComboBox

from snapred.ui.view.BackendRequestView import BackendRequestView


class NormalizationCalibrationRequestView(BackendRequestView):
    def __init__(self, jsonForm, samplePaths=[], parent=None):
        selection = "calibration/reduction"
        super(NormalizationCalibrationRequestView, self).__init__(jsonForm, selection, parent=parent)
        self.runNumberField = self._labeledField("Run Number", jsonForm.getField("runNumber"))
        self.emptyRunNumberField = self._labeledField("Empty Run Number", jsonForm.getField("emptyRunNumber"))
        self.smoothingParameter = self._labeledField("Smoothing Parameter", jsonForm.getField("smoothingParameter"))
        # might need to add inputs for absorption correciton here
        self.sampleDropDown = self._sampleDropDown("Sample", samplePaths)

        self.layout.addWidget(self.runNumberField, 0, 0)
        self.layout.addWidget(self.emptyRunNumberField, 0, 1)
        self.layout.addWidget(self.smoothingParameter, 1, 0)
        self.layout.addWidget(self.sampleDropDown, 1, 1)

    def verify(self):
        if self.sampleDropDown.currentIndex() == 0:
            raise ValueError("Please select a sample")
        return True
