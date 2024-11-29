from qtpy.QtCore import Signal, Slot
from qtpy.QtWidgets import QLabel

from snapred.meta.decorators.Resettable import Resettable
from snapred.ui.view.BackendRequestView import BackendRequestView


# TODO rebase on BackendRequestView
@Resettable
class NormalizationSaveView(BackendRequestView):
    """
    This class creates a qt widget interface for efficiently saving normalization data in SNAPRed
    after user assessment. It provides a structured and intuitive environment for users to input and
    review important details such as run numbers and versioning. By leveraging dynamic form generation
    and organized UI elements, it ensures data consistency and facilitates user interaction. The main
    goal is to streamline the confirmation and saving process of normalization data, enhancing both
    the user experience and the integrity of stored data.
    """

    signalRunNumberUpdate = Signal(str)
    signalBackgroundRunNumberUpdate = Signal(str)

    def __init__(self, parent=None):
        super(NormalizationSaveView, self).__init__(parent)
        self.interactionText = QLabel("Assessment Complete! Would you like to save the normalization now?")

        self.fieldRunNumber = self._labeledLineEdit("Run Number :")
        self.fieldRunNumber.setEnabled(False)
        self.signalRunNumberUpdate.connect(self._updateRunNumber)

        self.fieldBackgroundRunNumber = self._labeledLineEdit("Background Run Number :")
        self.fieldBackgroundRunNumber.setEnabled(False)
        self.signalBackgroundRunNumberUpdate.connect(self._updateBackgroundRunNumber)

        self.fieldVersion = self._labeledLineEdit("Version :")
        # add tooltip to leave blank for new version
        self.fieldVersion.setToolTip("Leave blank for new version!")

        self.fieldAppliesTo = self._labeledLineEdit("Applies To :")
        self.fieldAppliesTo.setToolTip(
            "Determines which runs this normalization applies to. 'runNumber', '>runNumber', or \
                '<runNumber', default is '>runNumber'."
        )

        self.fieldComments = self._labeledLineEdit("Comments :")
        self.fieldComments.setToolTip("Comments about the normalization, documentation of important information.")

        self.fieldAuthor = self._labeledLineEdit("Author :")
        self.fieldAuthor.setToolTip("Author of the normalization.")

        _layout = self.layout()
        _layout.addWidget(self.interactionText)
        _layout.addWidget(self.fieldRunNumber)
        _layout.addWidget(self.fieldBackgroundRunNumber)
        _layout.addWidget(self.fieldVersion)
        _layout.addWidget(self.fieldAppliesTo)
        _layout.addWidget(self.fieldComments)
        _layout.addWidget(self.fieldAuthor)

    @Slot(str)
    def _updateRunNumber(self, runNumber: str):
        self.fieldRunNumber.setText(runNumber)

    def updateRunNumber(self, runNumber: str):
        self.signalRunNumberUpdate.emit(runNumber)

    @Slot(str)
    def _updateBackgroundRunNumber(self, backgroundRunNumber: str):
        self.fieldBackgroundRunNumber.setText(backgroundRunNumber)

    def updateBackgroundRunNumber(self, backgroundRunNumber: str):
        self.signalBackgroundRunNumberUpdate.emit(backgroundRunNumber)

    def verify(self):
        if self.fieldAuthor.text() == "":
            raise ValueError("You must specify the author")
        if self.fieldComments.text() == "":
            raise ValueError("You must add comments")
        return True
