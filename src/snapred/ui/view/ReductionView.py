from qtpy.QtWidgets import QGridLayout, QPushButton, QWidget

from snapred.meta.decorators.Resettable import Resettable
from snapred.ui.widget.LabeledCheckBox import LabeledCheckBox
from snapred.ui.widget.LabeledField import LabeledField
from snapred.ui.widget.MultiRunDialog import MultiRunNumberDialog
from snapred.ui.widget.SampleDropDown import SampleDropDown
from snapred.ui.widget.Toggle import Toggle


@Resettable
class ReductionView(QWidget):
    def __init__(self, pixelMasks=[], parent=None):
        super().__init__(parent)
        self.layout = QGridLayout()
        self.setLayout(self.layout)

        self.runNumberField = LabeledField("Run Number:")
        self.litemodeToggle = LabeledField("Lite Mode", Toggle(parent=self, state=True))
        self.checkbox = LabeledCheckBox("Retain Unfocussed Data")
        self.pixelMaskDropdown = SampleDropDown("Pixel Masks", pixelMasks)
        self.multiRunDialogButton = QPushButton("Multi Run Numbers")
        self.multiRunDialogButton.clicked.connect(self.openMultiRunDialog)

        self.layout.addWidget(self.runNumberField, 0, 0)
        self.layout.addWidget(self.litemodeToggle, 0, 1)
        self.layout.addWidget(self.multiRunDialogButton, 1, 0)
        self.layout.addWidget(self.pixelMaskDropdown, 2, 0)
        self.layout.addWidget(self.checkbox, 2, 1)

        # connect the boolean signal to a slot
        # self.checkbox.checkedChanged.connect()

    def openMultiRunDialog(self):
        if not hasattr(self, "_multiRunDialogInstance"):
            self._multiRunDialogInstance = MultiRunNumberDialog(self)
        self._multiRunDialogInstance.clearRunNumberFields()
        runNumbers = [num.strip() for num in self.runNumberField.text().split(",") if num.strip()]
        for num in runNumbers:
            self._multiRunDialogInstance.addRunNumberField(num)
        self._multiRunDialogInstance.exec_()

    def getAllRunNumbers(self):
        allNumbers = []
        mainFieldNumbers = self.runNumberField.text().split(",")
        allNumbers.extend([num.strip() for num in mainFieldNumbers if num.strip()])
        if hasattr(self, "_multiRunDialogInstance"):
            allNumbers.extend(self._multiRunDialogInstance.getAllRunNumbers())
        return allNumbers

    def verify(self):
        allNumbers = self.getAllRunNumbers()
        if not all(allNumbers):
            raise ValueError("Please ensure all run number fields are filled.")
        for number in allNumbers:
            if not number.isdigit():
                raise ValueError("Invalid run number detected.")
        if self.pixelMaskDropdown.currentIndex() < 0:
            raise ValueError("Please select a pixel mask.")

    # place holder for checkBox logic
    """
    def onCheckBoxChecked(self, checked):
        if checked:

        else:
            pass
    """
