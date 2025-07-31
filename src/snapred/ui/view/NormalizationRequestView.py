from qtpy.QtCore import Signal, Slot

from snapred.backend.dao.RunMetadata import RunMetadata
from snapred.meta.decorators.Resettable import Resettable
from snapred.ui.view.BackendRequestView import BackendRequestView


@Resettable
class NormalizationRequestView(BackendRequestView):
    """

    The UI class in SNAPRed is designed to streamline the submission of normalization requests, enhancing the user
    experience within the platform. By extending BackendRequestView and incorporating the @Resettable decorator,
    it enables users to easily manage and reset normalization workflows as needed. Through intuitive features like
    run number fields, lite mode toggles, and dropdown menus for sample selection, the class ensures that users can
    provide necessary inputs effectively. Overall, its purpose is to simplify the normalization request process,
    facilitating efficient data processing and analysis within SNAPRed.

    """

    signalUpdateRunMetadata = Signal(RunMetadata)

    def __init__(self, samplePaths=[], groups=[], parent=None):
        super(NormalizationRequestView, self).__init__(parent=parent)

        self.signalUpdateRunMetadata.connect(self._setRunMetadata)

        # input fields
        self.runNumberField = self._labeledLineEdit("Run Number:")
        self.runNumberField.setToolTip("Vanadium sample run number to be normalized.")
        self.liteModeToggle = self._labeledToggle("Lite Mode", True)
        self.backgroundRunNumberField = self._labeledLineEdit("Background Run Number:")
        self.backgroundRunNumberField.setToolTip("Background run number to be subtracted from the sample run.")
        # drop downs
        self.sampleDropdown = self._sampleDropDown("Select Sample", samplePaths)
        self.sampleDropdown.setToolTip("Samples available for this run number.")
        self.groupingFileDropdown = self._sampleDropDown("Select Grouping File", groups)
        self.setDefaultGrouping(groups)
        self.groupingFileDropdown.setToolTip("Grouping schemas available for this sample run number.")

        # set field properties
        self.liteModeToggle.setEnabled(False)

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
        self.runMetadataStateId.field.setReadOnly(True)
        self.runMetadataStateId.setEnabled(False)
        self.runMetadataRunTitle.field.setReadOnly(True)
        self.runMetadataRunTitle.setEnabled(False)

        # add all widgets to layout
        _layout = self.layout()
        _layout.addWidget(self.runNumberField, 0, 0)
        _layout.addWidget(self.backgroundRunNumberField, 0, 1)
        _layout.addWidget(self.liteModeToggle, 0, 2)
        _layout.addWidget(self.runMetadataStateId, 1, 1)
        _layout.addWidget(self.runMetadataRunTitle, 1, 0)
        _layout.addWidget(self.sampleDropdown, 2, 0)
        _layout.addWidget(self.groupingFileDropdown, 2, 1)

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
        if not self.backgroundRunNumberField.text().isdigit():
            raise ValueError("Please enter a valid background run number")
        if self.sampleDropdown.currentIndex() < 0:
            raise ValueError("Please select a sample")
        if self.groupingFileDropdown.currentIndex() < 0:
            raise ValueError("Please select a grouping file")
        return True

    def getRunNumber(self):
        return self.runNumberField.text()

    def setInteractive(self, flag: bool):
        # TODO: put widgets here to allow them to be enabled or disabled by the presenter.
        self.runNumberField.setEnabled(flag)
        self.backgroundRunNumberField.setEnabled(flag)
        self.liteModeToggle.setEnabled(flag)
        self.sampleDropdown.setEnabled(flag)
        self.groupingFileDropdown.setEnabled(flag)

    def updateRunMetadata(self, metadata: RunMetadata):
        self.signalUpdateRunMetadata.emit(metadata)

    @Slot(RunMetadata)
    def _setRunMetadata(self, metadata: RunMetadata | None = None):
        stateId = ""
        runTitle = ""
        if metadata is not None:
            stateId = metadata.stateId.hex
            runTitle = metadata.runTitle
        self.runMetadataStateId.setText(stateId)
        self.runMetadataRunTitle.setText(runTitle)
