from PyQt5.QtCore import pyqtSignal
from PyQt5.QtWidgets import QGridLayout, QLabel, QWidget

from snapred.meta.decorators.Resettable import Resettable
from snapred.ui.widget.JsonFormList import JsonFormList
from snapred.ui.widget.LabeledField import LabeledField


# TODO rebase on BackendRequestView
@Resettable
class NormalizationSaveView(QWidget):
    """

    This class creates a PyQt5 widget interface for efficiently saving normalization data in SNAPRed
    after user assessment. It provides a structured and intuitive environment for users to input and
    review important details such as run numbers and versioning. By leveraging dynamic form generation
    and organized UI elements, it ensures data consistency and facilitates user interaction. The main
    goal is to streamline the confirmation and saving process of normalization data, enhancing both
    the user experience and the integrity of stored data.

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

    def verify(self):
        if self.fieldAuthor.text() == "":
            raise ValueError("You must specify the author")
        if self.fieldComments.text() == "":
            raise ValueError("You must add comments")
        return True
