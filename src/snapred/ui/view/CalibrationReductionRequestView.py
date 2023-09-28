from qtpy.QtWidgets import QComboBox

from snapred.ui.view.BackendRequestView import BackendRequestView
from snapred.ui.widget.Toggle import Toggle


class CalibrationReductionRequestView(BackendRequestView):
    def __init__(self, jsonForm, samples=[], groups=[], parent=None):
        selection = "calibration/reduction"
        super(CalibrationReductionRequestView, self).__init__(jsonForm, selection, parent=parent)
        self.runNumberField = self._labeledField("Run Number", jsonForm.getField("runNumber"))
        self.litemodeToggle = self._labeledField("Lite Mode", Toggle(parent=self, state=True))
        self.litemodeToggle.setEnabled(False)
        self.layout.addWidget(self.runNumberField, 0, 0)
        self.layout.addWidget(self.litemodeToggle, 0, 1)

        self.sampleDropdown = QComboBox()
        self.sampleDropdown.addItem("Select Sample")
        self.sampleDropdown.addItems(samples)
        self.sampleDropdown.model().item(0).setEnabled(False)
        # todo: get samples from backend
        self.groupingFileDropdown = QComboBox()
        self.groupingFileDropdown.addItem("Select Grouping File")
        self.groupingFileDropdown.addItems(groups)
        self.groupingFileDropdown.model().item(0).setEnabled(False)
        # todo get grouping files from backend
        self.layout.addWidget(self.sampleDropdown, 1, 0)
        self.layout.addWidget(self.groupingFileDropdown, 1, 1)
