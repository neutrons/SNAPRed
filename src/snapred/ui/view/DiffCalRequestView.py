from qtpy.QtCore import Signal, Slot

from snapred.backend.dao.RunMetadata import RunMetadata
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

    signalUpdatePeakFunctionIndex = Signal(int)
    signalUpdateRunMetadata = Signal(object)  # `Signal(RunMetadata | None)` as `Signal(object)`

    def __init__(self, samples=[], groups=[], parent=None):
        super().__init__(parent=parent)

        # connect internal signals
        self.signalUpdatePeakFunctionIndex.connect(self._setPeakFunctionIndex)
        self.signalUpdateRunMetadata.connect(self._setRunMetadata)

        # input fields
        self.runNumberField = self._labeledField("Run Number")
        self.runNumberField.setToolTip("Run number to be calibrated.")
        self.liteModeToggle = self._labeledToggle("Lite Mode", True)

        # drop downs
        self.sampleDropdown = self._sampleDropDown("Sample", samples)
        self.sampleDropdown.setToolTip("Samples available for this run number.")
        self.groupingFileDropdown = self._sampleDropDown("Grouping File", groups)
        self.groupingFileDropdown.setToolTip("Grouping schemas available for this sample run number.")
        self.setDefaultGrouping(groups)
        self.peakFunctionDropdown = self._sampleDropDown("Peak Function", [p.value for p in SymmetricPeakEnum])
        self.peakFunctionDropdown.setToolTip("Peak function to be used for calibration.")

        # set field properties
        self.liteModeToggle.setEnabled(True)
        # Default to Gaussian peak function
        self.peakFunctionDropdown.setCurrentIndex(0)

        # skip pixel calibration toggle
        self.skipPixelCalToggle = self._labeledToggle("Skip Pixel Calibration", False)

        # run number metadata fields
        stateIdLabel = "State ID:"
        self.runMetadataStateId = self._labeledField(stateIdLabel)
        self.runMetadataStateId.setToolTip("State ID of the run number.")
        # set max width to 16 characters (stateid length)
        charWidth = self.runMetadataStateId.fontMetrics().averageCharWidth()
        fieldWidth = charWidth * (16 + len(stateIdLabel)) + 20  # +20 for padding
        self.runMetadataStateId.setFixedWidth(fieldWidth)
        self.runMetadataRunTitle = self._labeledField("Run Title")
        self.runMetadataRunTitle.setToolTip("Title of the run from PV file.")

        # run metadata fields are read only
        self.runMetadataStateId.setEnabled(False)
        self.runMetadataRunTitle.setEnabled(False)

        # add all widgets to layout
        layout_ = self.layout()

        layout_.addWidget(self.runNumberField, 0, 0)
        layout_.addWidget(self.liteModeToggle, 0, 2)
        layout_.addWidget(self.runMetadataStateId, 1, 1)
        layout_.addWidget(self.runMetadataRunTitle, 1, 0)
        layout_.addWidget(self.skipPixelCalToggle, 1, 2)
        layout_.addWidget(self.sampleDropdown, 3, 0)
        layout_.addWidget(self.groupingFileDropdown, 3, 1)
        layout_.addWidget(self.peakFunctionDropdown, 3, 2)

    def setDefaultGrouping(self, groups):
        # find the first group where case insensitive match "column" is found
        for i, group in enumerate(groups):
            if "column" in group.lower():
                self.groupingFileDropdown.setCurrentIndex(i)
                break

    def populateGroupingDropdown(self, groups):
        self.groupingFileDropdown.setItems(groups)
        self.setDefaultGrouping(groups)

    def verify(self):
        if not self.runNumberField.text().isdigit():
            self._setRunMetadata()
            raise ValueError("Please enter a valid run number")
        if self.sampleDropdown.currentIndex() < 0:
            raise ValueError("Please select a sample")
        if self.groupingFileDropdown.currentIndex() < 0:
            raise ValueError("Please select a grouping file")
        if self.peakFunctionDropdown.currentIndex() < 0:
            raise ValueError("Please select a peak function")
        return True

    def setInteractive(self, flag: bool):
        # TODO: put widgets here to allow them to be enabled or disabled by the presenter.
        self.runNumberField.setEnabled(flag)
        self.sampleDropdown.setEnabled(flag)
        self.groupingFileDropdown.setEnabled(flag)
        self.peakFunctionDropdown.setEnabled(flag)

        self.liteModeToggle.setEnabled(flag)
        self.skipPixelCalToggle.setEnabled(flag)

    def getRunNumber(self):
        return self.runNumberField.text()

    def getLiteMode(self):
        return self.liteModeToggle.getState()

    def getSkipPixelCalibration(self):
        return self.skipPixelCalToggle.getState()

    def disablePeakFunction(self):
        self.peakFunctionDropdown.setEnabled(False)

    def enablePeakFunction(self):
        self.peakFunctionDropdown.setEnabled(True)

    def updatePeakFunctionIndex(self, index):
        self.signalUpdatePeakFunctionIndex.emit(index)

    @Slot(int)
    def _setPeakFunctionIndex(self, index):
        self.peakFunctionDropdown.setCurrentIndex(index)

    def updateRunMetadata(self, metadata: RunMetadata | None):
        self.signalUpdateRunMetadata.emit(metadata)

    @Slot(object)  # `Signal(RunMetadata | None)` as `Signal(object)`
    def _setRunMetadata(self, metadata: RunMetadata | None = None):
        stateId = ""
        runTitle = ""
        if metadata is not None:
            stateId = metadata.stateId.hex
            runTitle = metadata.runTitle
        self.runMetadataStateId.setText(stateId)
        self.runMetadataRunTitle.setText(runTitle)
