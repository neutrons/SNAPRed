from qtpy.QtWidgets import QComboBox

from snapred.ui.view.BackendRequestView import BackendRequestView
from snapred.ui.widget.Toggle import Toggle


class CalibrationReductionRequestView(BackendRequestView):
    def __init__(self, jsonForm, samples=[], groups=[], parent=None):
        selection = "calibration/reduction"
        super(CalibrationReductionRequestView, self).__init__(jsonForm, selection, parent=parent)
        self.runNumberField = self._labeledField("Run Number", jsonForm.getField("runNumber"))
        self.litemodeToggle = self._labeledField("Lite Mode", Toggle(parent=self, state=True))
        self.fieldConvergnceThreshold = self._labeledField(
            "Convergence Threshold", jsonForm.getField("convergenceThreshold")
        )
        self.fieldPeakIntensityThreshold = self._labeledField(
            "Peak Intensity Threshold", jsonForm.getField("peakIntensityThreshold")
        )
        self.fieldNBinsAcrossPeakWidth = self._labeledField(
            "Bins Across Peak Width", jsonForm.getField("nBinsAcrossPeakWidth")
        )

        self.litemodeToggle.setEnabled(False)
        self.layout.addWidget(self.runNumberField, 0, 0)
        self.layout.addWidget(self.litemodeToggle, 0, 1)
        self.layout.addWidget(self.fieldConvergnceThreshold, 1, 0)
        self.layout.addWidget(self.fieldPeakIntensityThreshold, 1, 1)
        self.layout.addWidget(self.fieldNBinsAcrossPeakWidth, 1, 2)

        self.sampleDropdown = QComboBox()
        self.sampleDropdown.addItem("Select Sample")
        self.sampleDropdown.addItems(samples)
        self.sampleDropdown.model().item(0).setEnabled(False)

        self.groupingFileDropdown = QComboBox()
        self.groupingFileDropdown.addItem("Select Grouping File")
        self.groupingFileDropdown.addItems(groups)
        self.groupingFileDropdown.model().item(0).setEnabled(False)

        self.layout.addWidget(self.sampleDropdown, 2, 0)
        self.layout.addWidget(self.groupingFileDropdown, 2, 1)

    def verify(self):
        if self.sampleDropdown.currentIndex() == 0:
            raise ValueError("Please select a sample")
        if self.groupingFileDropdown.currentIndex() == 0:
            raise ValueError("Please select a grouping file")
        return True
