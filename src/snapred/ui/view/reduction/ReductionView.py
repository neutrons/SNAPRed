from qtpy.QtCore import Signal, Slot
from qtpy.QtWidgets import QHBoxLayout, QLineEdit, QPushButton, QTextEdit, QVBoxLayout, QWidget
from snapred.meta.decorators.Resettable import Resettable
from snapred.ui.widget.LabeledCheckBox import LabeledCheckBox
from snapred.ui.widget.LabeledField import LabeledField
from snapred.ui.widget.SampleDropDown import SampleDropDown
from snapred.ui.widget.Toggle import Toggle


@Resettable
class ReductionView(QWidget):
    signalRemoveRunNumber = Signal(int)

    def __init__(self, pixelMasks=[], parent=None):
        super().__init__(parent)

        self.runNumbers = []

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
        self.pixelMaskDropdown = SampleDropDown("Pixel Masks", pixelMasks)

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
        runNumberList = self.parseInputRunNumbers()
        if runNumberList is not None:
            noDuplicates = set(self.runNumbers)
            noDuplicates.update(runNumberList)
            self.runNumbers = list(noDuplicates)
            self.updateRunNumberList()
            self.runNumberInput.clear()

    def parseInputRunNumbers(self):
        runNumberString = self.runNumberInput.text().strip()
        if runNumberString:
            try:
                runNumberList = [num.strip() for num in runNumberString.split(",") if num.strip().isdigit()]
                return runNumberList
            except ValueError:
                raise ValueError(
                    "Please enter a valid run number or list of run numbers. (e.g. 46680, 46685, 46686, etc...)"
                )

    @Slot()
    def removeRunNumber(self, runNumber):
        self.signalRemoveRunNumber.emit(runNumber)

    @Slot()
    def _removeRunNumber(self, runNumber):
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
        return True

    def getRunNumbers(self):
        return self.runNumbers

    # Placeholder for checkBox logic
    """
    def onCheckBoxChecked(self, checked):
        if checked:

        else:
            pass
    """
