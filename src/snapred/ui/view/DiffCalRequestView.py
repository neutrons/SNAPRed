from PyQt5.QtCore import pyqtSignal
from qtpy.QtWidgets import QComboBox, QLineEdit

from snapred.meta.decorators.Resettable import Resettable
from snapred.meta.mantid.AllowedPeakTypes import SymmetricPeakEnum
from snapred.ui.view.BackendRequestView import BackendRequestView
from snapred.ui.widget.Toggle import Toggle


@Resettable
class DiffCalRequestView(BackendRequestView):
    def __init__(self, jsonForm, samples=[], groups=[], parent=None):
        selection = "calibration/diffractionCalibration"
        super().__init__(jsonForm, selection, parent=parent)

        self.runNumberField = self._labeledField("Run Number")
        self.litemodeToggle = self._labeledField("Lite Mode", Toggle(parent=self, state=True))
        self.fieldConvergenceThreshold = self._labeledField("Convergence Threshold")
        self.fieldPeakIntensityThreshold = self._labeledField("Peak Intensity Threshold")

        self.fieldNBinsAcrossPeakWidth = self._labeledField("Bins Across Peak Width")
        self.sampleDropdown = self._sampleDropDown("Sample", samples)
        self.groupingFileDropdown = self._sampleDropDown("Grouping File", groups)
        self.peakFunctionDropdown = self._sampleDropDown("Peak Function", [p.value for p in SymmetricPeakEnum])

        self.litemodeToggle.setEnabled(True)
        self.peakFunctionDropdown.setCurrentIndex(0)

        self.layout.addWidget(self.runNumberField, 0, 0)
        self.layout.addWidget(self.litemodeToggle, 0, 1)

        self.layout.addWidget(self.fieldConvergenceThreshold, 1, 0)

        self.layout.addWidget(self.fieldPeakIntensityThreshold, 1, 1)
        self.layout.addWidget(self.fieldNBinsAcrossPeakWidth, 1, 2)
        self.layout.addWidget(self.sampleDropdown, 2, 0)
        self.layout.addWidget(self.groupingFileDropdown, 2, 1)
        self.layout.addWidget(self.peakFunctionDropdown, 2, 2)

    def populateGroupingDropdown(self, groups):
        self.groupingFileDropdown.setItems(groups)

    def verify(self):
        if self.sampleDropdown.currentIndex() < 0:
            raise ValueError("Please select a sample")
        if self.groupingFileDropdown.currentIndex() < 0:
            raise ValueError("Please select a grouping file")
        if self.groupingFileDropdown.currentIndex() < 0:
            raise ValueError("You must enter a run number to select a grouping defintion")
        if self.peakFunctionDropdown.currentIndex() < 0:
            raise ValueError("Please select a peak function")
        return True
