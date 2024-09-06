from typing import Callable, List, Optional

from qtpy.QtCore import Signal, Slot
from qtpy.QtWidgets import (
    QHBoxLayout,
    QLineEdit,
    QMessageBox,
    QPushButton,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)
from snapred.backend.log.logger import snapredLogger
from snapred.meta.decorators.Resettable import Resettable
from snapred.ui.widget.LabeledCheckBox import LabeledCheckBox
from snapred.ui.widget.LabeledField import LabeledField
from snapred.ui.widget.SampleDropDown import SampleDropDown
from snapred.ui.widget.Toggle import Toggle

logger = snapredLogger.getLogger(__name__)


@Resettable
class ReductionRequestView(QWidget):
    signalRemoveRunNumber = Signal(int)

    def __init__(self, parent=None, validateRunNumbers: Optional[Callable[[List[str]], None]] = None):
        super(ReductionRequestView, self).__init__(parent=parent)

        self.runNumbers = []
        self.validateRunNumbers = validateRunNumbers

        self.layout = QVBoxLayout()
        self.setLayout(self.layout)

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
        self.liteModeToggle = LabeledField("Lite Mode", Toggle(parent=self, state=True))
        self.retainUnfocusedDataCheckbox = LabeledCheckBox("Retain Unfocussed Data")

        # not active in this release, but kept as a "promise" for future releases
        self.pixelMaskDropdown = SampleDropDown("Pixel Masks", [])

        # Set field properties
        self.liteModeToggle.setEnabled(False)
        self.retainUnfocusedDataCheckbox.setEnabled(False)
        self.pixelMaskDropdown.setEnabled(False)

        # Add widgets to layout
        self.layout.addLayout(self.runNumberLayout)
        self.layout.addWidget(self.runNumberDisplay)
        self.layout.addWidget(self.liteModeToggle)
        self.layout.addWidget(self.pixelMaskDropdown)
        self.layout.addWidget(self.retainUnfocusedDataCheckbox)

        # Connect buttons to methods
        self.enterRunNumberButton.clicked.connect(self.addRunNumber)
        self.clearButton.clicked.connect(self.clearRunNumbers)

        self.signalRemoveRunNumber.connect(self._removeRunNumber)

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
        except ValueError as e:
            QMessageBox.warning(self, "Warning", str(e), buttons=QMessageBox.Ok, defaultButton=QMessageBox.Ok)
            self.runNumberInput.clear()

    def parseInputRunNumbers(self) -> List[str]:
        # WARNING: run numbers are strings.
        #   For now, it's OK to parse them as integer, but they should not be passed around that way.
        runNumberString = self.runNumberInput.text().strip()
        if runNumberString:
            runNumberList = [num.strip() for num in runNumberString.split(",") if num.strip()]
            for runNumber in runNumberList:
                if not runNumber.isdigit():
                    raise ValueError(
                        "Please enter a valid run number or list of run numbers. (e.g. 46680, 46685, 46686, etc...)"
                    )
            return runNumberList

    @Slot()
    def removeRunNumber(self, runNumber):
        self.signalRemoveRunNumber.emit(runNumber)

    @Slot()
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
        return True

    def getRunNumbers(self):
        return self.runNumbers
