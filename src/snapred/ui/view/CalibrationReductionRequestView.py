from qtpy.QtWidgets import QComboBox

from snapred.ui.view.BackendRequestView import BackendRequestView
from snapred.ui.widget.Toggle import Toggle


class CalibrationReductionRequestView(BackendRequestView):
    def __init__(self, jsonForm, parent=None):
        selection = "calibration/reduction"
        super(CalibrationReductionRequestView, self).__init__(jsonForm, selection, parent=parent)
        runNumberField = self._labeledField("Run Number", jsonForm.getField("runNumber"))
        litemodeToggle = self._labeledField("Lite Mode", Toggle(parent=self))
        self.layout.addWidget(runNumberField, 0, 0)
        self.layout.addWidget(litemodeToggle, 0, 1)

        sampleDropdown = QComboBox()
        sampleDropdown.addItem("Select Sample")
        # todo: get samples from backend
        groupingFileDropdown = QComboBox()
        groupingFileDropdown.addItem("Select Grouping File")
        # todo get grouping files from backend
        self.layout.addWidget(sampleDropdown, 1, 0)
        self.layout.addWidget(groupingFileDropdown, 1, 1)
