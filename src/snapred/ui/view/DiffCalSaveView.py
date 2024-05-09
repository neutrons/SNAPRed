from qtpy.QtCore import Signal
from qtpy.QtWidgets import QComboBox, QGridLayout, QLabel, QLineEdit, QWidget

from snapred.meta.decorators.Resettable import Resettable
from snapred.ui.widget.LabeledField import LabeledField


# TODO rebase on BackendRequestView
@Resettable
class DiffCalSaveView(QWidget):
    """

    The DiffCalSaveView is a Qt widget designed for the final step in the calibration process within
    the SNAPRed application, where users decide whether to save the completed calibration. It features
    an intuitive layout with fields for entering run number, version, applicability, comments, and author
    information, alongside a hidden iteration dropdown for selecting calibration iterations. This view
    emphasizes clear communication with the user, asking if they wish to save the calibration and providing
    tooltips for guidance on each field.

    """

    signalRunNumberUpdate = Signal(str)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.currentIterationText = "Current"
        self.layout = QGridLayout()
        self.setLayout(self.layout)

        self.interactionText = QLabel("Assessment Complete! Would you like to save the calibration now?")

        self.fieldRunNumber = LabeledField("Run Number :", QLineEdit(parent=self), self)
        self.fieldRunNumber.setEnabled(False)
        self.signalRunNumberUpdate.connect(self._updateRunNumber)

        self.fieldVersion = LabeledField("Version :", QLineEdit(parent=self), self)
        # add tooltip to leave blank for new version
        self.fieldVersion.setToolTip("Leave blank for new version!")

        self.fieldAppliesTo = LabeledField("Applies To :", QLineEdit(parent=self), self)
        self.fieldAppliesTo.setToolTip(
            "Determines which runs this calibration applies to. 'runNumber', '>runNumber', or \
                '<runNumber', default is '>runNumber'."
        )

        self.fieldComments = LabeledField("Comments :", QLineEdit(parent=self), self)
        self.fieldComments.setToolTip("Comments about the calibration, documentation of important information.")

        self.fieldAuthor = LabeledField("Author :", QLineEdit(parent=self), self)
        self.fieldAuthor.setToolTip("Author of the calibration.")

        self.iterationDropdown = QComboBox(parent=self)
        self.iterationWidget = LabeledField("Iteration :", self.iterationDropdown, self)
        self.iterationWidget.setVisible(False)

        self.layout.addWidget(self.interactionText)
        self.layout.addWidget(self.fieldRunNumber)
        self.layout.addWidget(self.fieldVersion)
        self.layout.addWidget(self.fieldAppliesTo)
        self.layout.addWidget(self.fieldComments)
        self.layout.addWidget(self.fieldAuthor)

    # This signal boilerplate mumbo jumbo is necessary because worker threads cant update the gui directly
    # So we have to send a signal to the main thread to update the gui, else we get an unhelpful segfault
    def _updateRunNumber(self, runNumber):
        self.fieldRunNumber.setText(runNumber)

    def updateRunNumber(self, runNumber):
        self.signalRunNumberUpdate.emit(runNumber)

    def enableIterationDropdown(self):
        self.iterationWidget.setVisible(True)
        self.layout.addWidget(self.iterationWidget)

    def setIterationDropdown(self, iterations):
        self.iterationDropdown.clear()
        self.iterationDropdown.addItems(iterations)
        self.iterationDropdown.setItemText(0, self.currentIterationText)

    def verify(self):
        if self.fieldAuthor.text() == "":
            raise ValueError("You must specify the author")
        if self.fieldComments.text() == "":
            raise ValueError("You must add comments")
        return True
