from typing import Callable, List, Optional

from qtpy.QtCore import Signal
from qtpy.QtWidgets import QHBoxLayout, QLineEdit, QPushButton, QTextEdit, QVBoxLayout
from snapred.backend.log.logger import snapredLogger
from snapred.meta.decorators.Resettable import Resettable
from snapred.ui.view.BackendRequestView import BackendRequestView
from snapred.ui.widget.Toggle import Toggle

logger = snapredLogger.getLogger(__name__)


@Resettable
class ReductionRequestView(BackendRequestView):
    signalRemoveRunNumber = Signal(int)

    def __init__(self, parent=None, populatePixelMaskDropdown: Optional[Callable[[], None]] = None):
        super(ReductionRequestView, self).__init__(parent=parent)

        self.runNumbers = []
        self.pixelMaskDropdown = self._multiSelectDropDown("Select Pixel Mask(s)", [])
        self.populatePixelMaskDropdown = populatePixelMaskDropdown

        # Horizontal layout for run number input and button
        self.runNumberLayout = QHBoxLayout()
        self.runNumberInput = QLineEdit()
        self.enterRunNumberButton = QPushButton("Enter Run Number")
        self.clearButton = QPushButton("Clear")
        self.runNumberButtonLayout = QVBoxLayout()
        self.runNumberButtonLayout.addWidget(self.enterRunNumberButton)
        self.runNumberButtonLayout.addWidget(self.clearButton)

        self.runNumberLayout.addWidget(self.runNumberInput)
        self.runNumberLayout.addLayout(self.runNumberButtonLayout)

        # Run number display
        self.runNumberDisplay = QTextEdit()
        self.runNumberDisplay.setReadOnly(True)

        # Lite mode toggle, pixel masks dropdown, and retain unfocused data checkbox
        self.liteModeToggle = self._labeledField("Lite Mode", Toggle(parent=self, state=True))
        self.retainUnfocusedDataCheckbox = self._labeledCheckBox("Retain Unfocused Data")
        self.convertUnitsDropdown = self._sampleDropDown(
            "Convert Units", ["TOF", "dSpacing", "Wavelength", "MomentumTransfer"]
        )

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

        self.signalRemoveRunNumber.connect(self._removeRunNumber)

    def addRunNumber(self):
        runNumberList = self.parseInputRunNumbers()
        if runNumberList is not None:
            # remove duplicates
            noDuplicates = set(self.runNumbers)
            noDuplicates.update(runNumberList)
            self.runNumbers = list(noDuplicates)
            self.updateRunNumberList()
            self.runNumberInput.clear()
            if self.populatePixelMaskDropdown:
                self.populatePixelMaskDropdown()

    def parseInputRunNumbers(self) -> List[str]:
        # WARNING: run numbers are strings.
        #   For now, it's OK to parse them as integer, but they should not be passed around that way.
        runNumberString = self.runNumberInput.text().strip()
        if runNumberString:
            try:
                runNumberList = [num.strip() for num in runNumberString.split(",") if num.strip().isdigit()]
                return runNumberList
            except ValueError:
                raise ValueError(
                    "Please enter a valid run number or list of run numbers. (e.g. 46680, 46685, 46686, etc...)"
                )

    def removeRunNumber(self, runNumber):
        self.signalRemoveRunNumber.emit(runNumber)

    def _removeRunNumber(self, runNumber):
        if runNumber not in self.runNumbers:
            logger.warning(
                f"[ReductionRequestView]: attempting to remove run {runNumber} not in the list {self.runNumbers}"
            )
            return
        self.runNumbers.remove(runNumber)
        self.updateRunNumberList()

    def updateRunNumberList(self):
        self.runNumberDisplay.setText("\n".join(map(str, sorted(self.runNumbers))))

    def clearRunNumbers(self):
        self.runNumbers.clear()
        self.runNumberDisplay.clear()

    def verify(self):
        currentText = self.runNumberDisplay.toPlainText()
        runNumbers = [num.strip() for num in currentText.split("\n") if num.strip()]
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

    # Placeholder for checkBox logic
    """
    def onCheckBoxChecked(self, checked):
        if checked:

        else:
            pass
    """
