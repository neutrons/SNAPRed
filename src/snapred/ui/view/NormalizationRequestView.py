from snapred.meta.decorators.Resettable import Resettable
from snapred.ui.view.BackendRequestView import BackendRequestView
from snapred.ui.widget.Toggle import Toggle


@Resettable
class NormalizationRequestView(BackendRequestView):
    """
    Constructs and manages the UI for submitting normalization requests within SNAPRed, extending
    BackendRequestView. This class is decorated with @Resettable, supporting reset operations to
    clear and restart the workflow as needed.

    Key Components and Functionalities:

    - Initializes with paths for calibration and diffraction calibration, incorporating UI elements
      from a provided JSON form structure.
    - Facilitates user input through fields for run numbers, lite mode toggle, and background run
      numbers, leveraging jsonForm data.
    - Provides dropdown menus for sample and grouping file selection, populated with samplePaths
      and groups respectively.
    - Arranges UI elements thoughtfully within the layout to enhance user experience.

    UI Elements:

    - Run Number Field: Allows input of the run identifier for normalization.
    - Lite Mode Toggle: Toggle switch to enable or disable lite mode, optimizing resource usage.
    - Background Run Number Field: Field for inputting the associated background run's identifier.
    - Sample Dropdown: Enables sample selection from predefined paths.
    - Grouping File Dropdown: Allows for the selection of a grouping file from available options.

    Functions include:

    - populateGroupingDropdown: Dynamically updates grouping file dropdown items.
    - verify: Validates user inputs, ensuring all required fields are completed before submission.
    - getRunNumber: Retrieves the run number from user input for processing.

    This class effectively bridges user inputs and the normalization request process, ensuring a
    seamless and intuitive interface for initiating normalization operations.

    """

    def __init__(self, jsonForm, samplePaths=[], groups=[], parent=None):
        selection = "calibration/diffractionCalibration"
        super(NormalizationRequestView, self).__init__(jsonForm, selection, parent=parent)
        self.runNumberField = self._labeledField("Run Number:", jsonForm.getField("runNumber"))
        self.litemodeToggle = self._labeledField("Lite Mode", Toggle(parent=self, state=True))
        self.backgroundRunNumberField = self._labeledField(
            "Background Run Number:", jsonForm.getField("backgroundRunNumber")
        )

        self.litemodeToggle.setEnabled(False)
        self.layout.addWidget(self.runNumberField, 0, 0)
        self.layout.addWidget(self.litemodeToggle, 0, 1)
        self.layout.addWidget(self.backgroundRunNumberField, 1, 0)

        self.sampleDropdown = self._sampleDropDown("Select Sample", samplePaths)
        self.groupingFileDropdown = self._sampleDropDown("Select Grouping File", groups)

        self.layout.addWidget(self.sampleDropdown, 2, 0)
        self.layout.addWidget(self.groupingFileDropdown, 2, 1)

    def populateGroupingDropdown(self, groups):
        self.groupingFileDropdown.setItems(groups)

    def verify(self):
        if self.sampleDropdown.currentIndex() < 0:
            raise ValueError("Please select a sample")
        if self.groupingFileDropdown.currentIndex() < 0:
            raise ValueError("Please select a grouping file")
        if self.groupingFileDropdown.currentIndex() < 0:
            raise ValueError("You must enter a run number to select a grouping defintion")
        if self.runNumberField.text() == "":
            raise ValueError("Please enter a run number")
        if self.backgroundRunNumberField.text() == "":
            raise ValueError("Please enter a background run number")
        return True

    def getRunNumber(self):
        return self.runNumberField.text()
