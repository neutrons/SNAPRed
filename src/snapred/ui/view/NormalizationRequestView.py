from snapred.backend.dao.SNAPResponse import ResponseCode, SNAPResponse
from snapred.meta.decorators.Resettable import Resettable
from snapred.ui.view.BackendRequestView import BackendRequestView
from snapred.ui.widget.Toggle import Toggle


@Resettable
class NormalizationRequestView(BackendRequestView):
    def __init__(self, jsonForm, samplePaths=[], groups=[], parent=None):
        super(NormalizationRequestView, self).__init__(jsonForm, "", parent=parent)

        # input fields
        self.runNumberField = self._labeledField("Run Number:", jsonForm.getField("runNumber"))
        self.litemodeToggle = self._labeledField("Lite Mode", Toggle(parent=self, state=True))
        self.backgroundRunNumberField = self._labeledField(
            "Background Run Number:", jsonForm.getField("backgroundRunNumber")
        )

        # drop downs
        self.sampleDropdown = self._sampleDropDown("Select Sample", samplePaths)
        self.groupingFileDropdown = self._sampleDropDown("Select Grouping File", groups)

        # set field properties
        self.litemodeToggle.setEnabled(False)

        # add all widgets to layout
        self.layout.addWidget(self.runNumberField, 0, 0)
        self.layout.addWidget(self.litemodeToggle, 0, 1)
        self.layout.addWidget(self.backgroundRunNumberField, 1, 0)
        self.layout.addWidget(self.sampleDropdown, 2, 0)
        self.layout.addWidget(self.groupingFileDropdown, 2, 1)

    def populateGroupingDropdown(self, groups):
        self.groupingFileDropdown.setItems(groups)

    def verify(self):
        if not self.runNumberField.text().isdigit():
            raise ValueError("Please enter a valid run number")
        if not self.backgroundRunNumberField.text().isdigit():
            raise ValueError("Please enter a valid background run number")
        if self.sampleDropdown.currentIndex() < 0:
            raise ValueError("Please select a sample")
        if self.groupingFileDropdown.currentIndex() < 0:
            raise ValueError("Please select a grouping file")
        return SNAPResponse(code=ResponseCode.OK, data=True)

    def getRunNumber(self):
        return self.runNumberField.text()
