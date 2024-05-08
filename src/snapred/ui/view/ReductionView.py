from qtpy.QtWidgets import QGridLayout, QWidget

from snapred.meta.decorators.Resettable import Resettable
from snapred.ui.widget.LabeledCheckBox import LabeledCheckBox
from snapred.ui.widget.LabeledField import LabeledField
from snapred.ui.widget.SampleDropDown import SampleDropDown
from snapred.ui.widget.Toggle import Toggle


@Resettable
class ReductionView(QWidget):
    # This class will need to updated once backend implemenation is complete.
    def __init__(self, pixelMasks=[], parent=None):
        super().__init__(parent)

        self.layout = QGridLayout()
        self.setLayout(self.layout)

        # input fields
        self.runNumberField = LabeledField("Run Number:")
        self.litemodeToggle = LabeledField("Lite Mode", Toggle(parent=self, state=True))
        self.checkbox = LabeledCheckBox("Retain Unfocussed Data")
        self.pixelMaskDropdown = SampleDropDown("Pixel Masks", pixelMasks)

        # set field properties
        self.litemodeToggle.setEnabled(False)
        self.checkbox.setEnabled(False)
        self.pixelMaskDropDdown.setEnabled(False)

        # connect the boolean signal to a slot
        # self.checkbox.checkedChanged.connect()

        # add all widgets to layout
        self.layout.addWidget(self.runNumberField, 0, 0)
        self.layout.addWidget(self.litemodeToggle, 0, 1)
        self.layout.addWidget(self.pixelMaskDropdown, 1, 0)
        self.layout.addWidget(self.checkbox, 1, 1)

    def verify(self):
        runNumbers = self.runNumberField.text().split(",")
        for runNumber in runNumbers:
            if not runNumber.strip().isdigit():
                raise ValueError(
                    "Please enter a valid run number or list of run numbers. (e.g. 46680, 46685, 46686, etc...)"
                )
        if self.pixelMaskDropdown.currentIndex() < 0:
            raise ValueError("Please select a pixel mask.")
        return True

    # place holder for checkBox logic
    """
    def onCheckBoxChecked(self, checked):
        if checked:

        else:
            pass
    """
