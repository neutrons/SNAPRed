from snapred.meta.Config import Config
from snapred.meta.decorators.Resettable import Resettable
from snapred.meta.mantid.AllowedPeakTypes import SymmetricPeakEnum
from snapred.ui.view.BackendRequestView import BackendRequestView


@Resettable
class DiffCalRequestView(BackendRequestView):
    """

    The DiffCalRequestView is a specialized view within the SNAPRed application,
    designed for configuring and submitting diffraction calibration requests.
    Adorned with the Resettable decorator for dynamic UI adjustments, it integrates
    various input fields, toggles, and dropdown menus, including options for run number,
    lite mode activation, convergence and peak intensity thresholds, and peak function
    selection from predefined symmetric peak types. This view not only facilitates the
    precise specification of calibration parameters by the user but also ensures the inputs'
    validity through a comprehensive verification process.

    """

    def __init__(self, samples=[], groups=[], parent=None):
        super().__init__(parent=parent)

        # input fields
        self.runNumberField = self._labeledField("Run Number")
        self.litemodeToggle = self._labeledToggle("Lite Mode", True)
        self.fieldConvergenceThreshold = self._labeledField("Convergence Threshold")
        self.fieldNBinsAcrossPeakWidth = self._labeledField("Bins Across Peak Width")

        # drop downs
        self.sampleDropdown = self._sampleDropDown("Sample", samples)
        self.groupingFileDropdown = self._sampleDropDown("Grouping File", groups)
        self.peakFunctionDropdown = self._sampleDropDown("Peak Function", [p.value for p in SymmetricPeakEnum])

        # checkbox for removing background
        # NOTE not enabled unless in CIS mode until remove event background is fixed -- then re-enable
        self.removeBackgroundToggle = self._labeledToggle("RemoveBackground", False)
        self.removeBackgroundToggle.setEnabled(Config["cis_mode"])

        # set field properties
        self.litemodeToggle.setEnabled(True)
        self.peakFunctionDropdown.setCurrentIndex(0)

        # skip pixel calibration toggle
        self.skipPixelCalToggle = self._labeledToggle("Skip Pixel Calibration", False)

        # add all widgets to layout
        self.layout.addWidget(self.runNumberField, 0, 0)
        self.layout.addWidget(self.litemodeToggle, 0, 1)
        self.layout.addWidget(self.skipPixelCalToggle, 0, 2)
        self.layout.addWidget(self.fieldConvergenceThreshold, 1, 0)
        self.layout.addWidget(self.fieldNBinsAcrossPeakWidth, 1, 1)
        self.layout.addWidget(self.removeBackgroundToggle, 1, 2)
        self.layout.addWidget(self.sampleDropdown, 2, 0)
        self.layout.addWidget(self.groupingFileDropdown, 2, 1)
        self.layout.addWidget(self.peakFunctionDropdown, 2, 2)

    def populateGroupingDropdown(self, groups):
        self.groupingFileDropdown.setItems(groups)

    def verify(self):
        if not self.runNumberField.text().isdigit():
            raise ValueError("Please enter a valid run number")
        if self.sampleDropdown.currentIndex() < 0:
            raise ValueError("Please select a sample")
        if self.groupingFileDropdown.currentIndex() < 0:
            raise ValueError("Please select a grouping file")
        if self.peakFunctionDropdown.currentIndex() < 0:
            raise ValueError("Please select a peak function")
        return True

    def getRunNumber(self):
        return self.runNumberField.text()

    def getLiteMode(self):
        return self.litemodeToggle.getState()

    def getRemoveBackground(self):
        return self.removeBackgroundToggle.getState()

    def getSkipPixelCalibration(self):
        return self.skipPixelCalToggle.getState()
