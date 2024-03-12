from PyQt5.QtCore import pyqtSignal
from PyQt5.QtWidgets import QGridLayout, QLabel, QWidget

from snapred.meta.decorators.Resettable import Resettable
from snapred.ui.widget.JsonFormList import JsonFormList
from snapred.ui.widget.LabeledField import LabeledField


@Resettable
class NormalizationSaveView(QWidget):
    """
    Provides a PyQt5 widget interface for saving normalization data post-assessment within SNAPRed.
    Adorned with the @Resettable decorator, this class enables users to input and review
    normalization information such as run numbers, versioning, and comments in a structured and
    user-friendly manner prior to finalizing the save operation.

    Components and Functionalities:

    - Inherits from QWidget, establishing a graphical interface component.
    - Utilizes JsonFormList to dynamically generate form fields based on a provided JSON schema map,
      enhancing data consistency and validation.
    - Employs a QGridLayout to efficiently organize UI elements.

    UI Elements:

    - Interaction Text: Guides the user with a prompt regarding the saving of normalization data.
    - Field Elements: LabeledField widgets for inputting/displaying normalization information like
      run numbers, version, applicability, comments, and authorship. Certain fields are disabled or
      equipped with tooltips to guide user input.
    - Fields are interconnected with pyqtSignals for updates from asynchronous operations, ensuring
      thread-safe UI interactions.

    Signal-Slot Mechanism:

    - Implements pyqtSignal instances for thread-safe UI updates related to run numbers.
    - Slots `_updateRunNumber` and `_updateBackgroundRunNumber` receive signals to update UI
      elements with provided values, facilitating smooth and safe UI interactions.

    This class streamlines the process of saving normalization data, offering a clear and efficient
    interface for users to review and confirm the details of normalization operations before
    committing them to persistent storage.

    """

    signalRunNumberUpdate = pyqtSignal(str)
    signalBackgroundRunNumberUpdate = pyqtSignal(str)

    def __init__(self, name, jsonSchemaMap, parent=None):
        super().__init__(parent)
        self._jsonFormList = JsonFormList(name, jsonSchemaMap, parent=parent)

        self.layout = QGridLayout()
        self.setLayout(self.layout)

        self.interactionText = QLabel("Assessment Complete! Would you like to save the normalization now?")

        self.fieldRunNumber = LabeledField(
            "Run Number :", self._jsonFormList.getField("normalizationIndexEntry.runNumber"), self
        )
        self.fieldRunNumber.setEnabled(False)
        self.signalRunNumberUpdate.connect(self._updateRunNumber)

        self.fieldBackgroundRunNumber = LabeledField(
            "Background Run Number :", self._jsonFormList.getField("normalizationIndexEntry.backgroundRunNumber"), self
        )
        self.fieldBackgroundRunNumber.setEnabled(False)
        self.signalBackgroundRunNumberUpdate.connect(self._updateBackgroundRunNumber)

        self.fieldVersion = LabeledField(
            "Version :", self._jsonFormList.getField("normalizationIndexEntry.version"), self
        )
        # add tooltip to leave blank for new version
        self.fieldVersion.setToolTip("Leave blank for new version!")

        self.fieldAppliesTo = LabeledField(
            "Applies To :", self._jsonFormList.getField("normalizationIndexEntry.appliesTo"), self
        )
        self.fieldAppliesTo.setToolTip(
            "Determines which runs this normalization applies to. 'runNumber', '>runNumber', or \
                '<runNumber', default is '>runNumber'."
        )

        self.fieldComments = LabeledField(
            "Comments :", self._jsonFormList.getField("normalizationIndexEntry.comments"), self
        )
        self.fieldComments.setToolTip("Comments about the normalization, documentation of important information.")

        self.fieldAuthor = LabeledField("Author :", self._jsonFormList.getField("normalizationIndexEntry.author"), self)
        self.fieldAuthor.setToolTip("Author of the normalization.")

        self.layout.addWidget(self.interactionText)
        self.layout.addWidget(self.fieldRunNumber)
        self.layout.addWidget(self.fieldBackgroundRunNumber)
        self.layout.addWidget(self.fieldVersion)
        self.layout.addWidget(self.fieldAppliesTo)
        self.layout.addWidget(self.fieldComments)
        self.layout.addWidget(self.fieldAuthor)

    def _updateRunNumber(self, runNumber):
        self.fieldRunNumber.setText(runNumber)

    def updateRunNumber(self, runNumber):
        self.signalRunNumberUpdate.emit(runNumber)

    def _updateBackgroundRunNumber(self, backgroundRunNumber):
        self.fieldBackgroundRunNumber.setText(backgroundRunNumber)

    def updateBackgroundRunNumber(self, backgroundRunNumber):
        self.signalBackgroundRunNumberUpdate.emit(backgroundRunNumber)
