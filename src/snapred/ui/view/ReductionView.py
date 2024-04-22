from snapred.meta.decorators.Resettable import Resettable
from snapred.ui.view.BackendRequestView import BackendRequestView
from snapred.ui.widget.Toggle import Toggle


@Resettable
class ReductionView(BackendRequestView):
    def __init__(self, jsonForm, pixelMasks=[], parent=None):
        super(ReductionView, self).__init__(jsonForm, "", parent=parent)

        # input fields
        self.runNumberField = self._labeledField("Run Number:", jsonForm.getField("runNumber"), """multi=True""")
        # multi argument allows acceptance of a list of run numbers like 12345, 67890, 12145, etc....
        self.litemodeToggle = self._labeledField("Lite Mode", Toggle(parent=self, state=True))
        self.checkbox = self._labeledCheckBox("Retain Unfocussed Data")
        self.dropDown = self._sampleDropDown("Pixel Masks", pixelMasks)

        # set field properties
        self.litemodeToggle.setEnabled(False)
        self.checkbox.setEnabled(False)
        self.dropDown.setEnabled(False)

        # connect the boolean signal to a slot
        # self.checkbox.checkedChanged.connect()

        # add all widgets to layout
        self.layout.addWidget(self.runNumberField, 0, 0)
        self.layout.addWidget(self.litemodeToggle, 0, 1)
        self.layout.addWidget(self.dropDown, 1, 0)
        self.layout.addWidget(self.checkbox, 1, 1)

    def verify(self):
        if not self.runNumberField.text().isdigit():
            raise ValueError("Please enter a valid run number")
        if self.dropDown.currentIndex() < 0:
            raise ValueError("Please select a pixel mask")
        return True

    # place holder for checkBox logic
    """
    def onCheckBoxChecked(self, checked):
        if checked:

        else:
            pass
    """
