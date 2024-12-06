from typing import Callable, List, Optional

from qtpy.QtCore import Slot
from qtpy.QtWidgets import (
    QHBoxLayout,
    QLineEdit,
    QListWidget,
    QMessageBox,
    QPushButton,
    QVBoxLayout,
)

from snapred.backend.dao.state.RunNumber import RunNumber
from snapred.backend.log.logger import snapredLogger
from snapred.meta.decorators.Resettable import Resettable
from snapred.ui.view.BackendRequestView import BackendRequestView

logger = snapredLogger.getLogger(__name__)


@Resettable
class ReductionRequestView(BackendRequestView):
    def __init__(
        self,
        parent=None,
        populatePixelMaskDropdown: Optional[Callable[[], None]] = None,
        validateRunNumbers: Optional[Callable[[List[str]], None]] = None,
    ):
        super(ReductionRequestView, self).__init__(parent=parent)

        self.runNumbers = []
        self.pixelMaskDropdown = self._multiSelectDropDown("Select Pixel Mask(s)", [])
        self.populatePixelMaskDropdown = populatePixelMaskDropdown
        self.validateRunNumbers = validateRunNumbers

        # Horizontal layout for run number input and button
        self.runNumberLayout = QHBoxLayout()
        self.runNumberInput = QLineEdit()
        self.runNumberInput.returnPressed.connect(self.addRunNumber)
        self.enterRunNumberButton = QPushButton("Enter Run Number")
        self.clearButton = QPushButton("Clear")
        self.runNumberButtonLayout = QVBoxLayout()
        self.runNumberButtonLayout.addWidget(self.enterRunNumberButton)
        self.runNumberButtonLayout.addWidget(self.clearButton)

        self.runNumberLayout.addWidget(self.runNumberInput)
        self.runNumberLayout.addLayout(self.runNumberButtonLayout)

        # Run number display
        self.runNumberDisplay = QListWidget()
        self.runNumberDisplay.setSortingEnabled(False)

        # Lite mode toggle, pixel masks dropdown, and retain unfocused data checkbox
        self.liteModeToggle = self._labeledToggle("Lite Mode", True)
        self.retainUnfocusedDataCheckbox = self._labeledCheckBox("Retain Unfocused Data")
        self.convertUnitsDropdown = self._sampleDropDown(
            "Convert Units", ["TOF", "dSpacing", "Wavelength", "MomentumTransfer"]
        )
        self.convertUnitsDropdown.setCurrentIndex(1)

        # Set field properties
        self.liteModeToggle.setEnabled(False)
        self.retainUnfocusedDataCheckbox.setEnabled(False)
        self.pixelMaskDropdown.setEnabled(False)
        self.convertUnitsDropdown.setEnabled(False)

        # Add widgets to layout
        self.layout.addLayout(self.runNumberLayout, 0, 0)
        self.layout.addWidget(self.runNumberDisplay)
        self.layout.addWidget(self.liteModeToggle)
        self.layout.addWidget(self.pixelMaskDropdown)
        self.layout.addWidget(self.retainUnfocusedDataCheckbox)
        self.layout.addWidget(self.convertUnitsDropdown)

        # Connect buttons to methods
        self.enterRunNumberButton.clicked.connect(self.addRunNumber)
        self.clearButton.clicked.connect(self.clearRunNumbers)

    @Slot()
    def addRunNumber(self):
        # TODO: FIX THIS!
        #   We're not inside the SNAPResponseHandler here, so we can't just throw a `ValueError`.
        try:
            runNumberList = self.parseInputRunNumbers()
            if runNumberList is not None:
                # remove duplicates
                noDuplicates = set(self.runNumbers)
                noDuplicates.update(runNumberList)
                noDuplicates = list(noDuplicates)
                if self.validateRunNumbers is not None:
                    self.validateRunNumbers(noDuplicates)
                self.runNumbers = noDuplicates
                self.updateRunNumberList()
                self.runNumberInput.clear()
                if self.populatePixelMaskDropdown is not None:
                    self.populatePixelMaskDropdown()
        except ValueError as e:
            QMessageBox.warning(self, "Warning", str(e), buttons=QMessageBox.Ok, defaultButton=QMessageBox.Ok)
            self.runNumberInput.clear()

    def parseInputRunNumbers(self) -> List[str]:
        # WARNING: run numbers are strings.
        #   For now, it's OK to parse them as integer, but they should not be passed around that way.
        try:
            runs, errors = RunNumber.runsFromIntArrayProperty(self.runNumberInput.text(), False)

            if len(errors) > 0:
                messageBox = QMessageBox(
                    QMessageBox.Warning,
                    "Warning",
                    "There are issues with some run(s)",
                    QMessageBox.Ok,
                    self,
                )
                formattedErrors = "\n\n".join([error[1] for error in errors])
                messageBox.setDetailedText(f"{formattedErrors}")
                messageBox.exec()
        except Exception:  # noqa BLE001
            raise ValueError(
                "Bad input was given for Reduction runs,"
                "please read mantid docs for IntArrayProperty on how to format input"
            )

        return [str(num) for num in runs]

    def updateRunNumberList(self):
        self.runNumberDisplay.clear()
        self.runNumberDisplay.addItems(self.runNumbers)

    def clearRunNumbers(self):
        self.runNumbers.clear()
        self.runNumberDisplay.clear()
        self.pixelMaskDropdown.setItems([])

    def verify(self):
        runNumbers = [self.runNumberDisplay.item(x).text() for x in range(self.runNumberDisplay.count())]
        if not runNumbers:
            raise ValueError("Please enter at least one run number.")
        if runNumbers != self.runNumbers:
            raise ValueError("Unexpected issue verifying run numbers.  Please clear and re-enter.")
        for runNumber in runNumbers:
            if not runNumber.isdigit():
                raise ValueError(
                    "Please enter a valid run number or list of run numbers. (e.g. 46680, 46685, 46686, etc...)"
                )
        # They dont need to select a pixel mask
        # if self.pixelMaskDropdown.currentIndex() < 0:
        #     raise ValueError("Please select a pixel mask.")
        if self.retainUnfocusedDataCheckbox.isChecked():
            if self.convertUnitsDropdown.currentIndex() < 0:
                raise ValueError("Please select units to convert to")
        return True

    def getRunNumbers(self):
        return self.runNumbers

    def getPixelMasks(self):
        return self.pixelMaskDropdown.checkedItems()
