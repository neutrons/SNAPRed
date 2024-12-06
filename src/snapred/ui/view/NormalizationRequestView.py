from snapred.meta.decorators.Resettable import Resettable
from snapred.ui.view.BackendRequestView import BackendRequestView


@Resettable
class NormalizationRequestView(BackendRequestView):
    """

    The UI class in SNAPRed is designed to streamline the submission of normalization requests, enhancing the user
    experience within the platform. By extending BackendRequestView and incorporating the @Resettable decorator,
    it enables users to easily manage and reset normalization workflows as needed. Through intuitive features like
    run number fields, lite mode toggles, and dropdown menus for sample selection, the class ensures that users can
    provide necessary inputs effectively. Overall, its purpose is to simplify the normalization request process,
    facilitating efficient data processing and analysis within SNAPRed.

    """

    def __init__(self, samplePaths=[], groups=[], parent=None):
        super(NormalizationRequestView, self).__init__(parent=parent)

        # input fields
        self.runNumberField = self._labeledLineEdit("Run Number:")
        self.litemodeToggle = self._labeledToggle("Lite Mode", True)
        self.backgroundRunNumberField = self._labeledLineEdit("Background Run Number:")

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
        return True

    def getRunNumber(self):
        return self.runNumberField.text()
