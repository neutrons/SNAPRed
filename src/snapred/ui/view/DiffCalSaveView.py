from qtpy.QtCore import Signal, Slot
from qtpy.QtWidgets import QComboBox, QLabel

from snapred.backend.dao.indexing.IndexEntry import IndexEntry
from snapred.backend.dao.indexing.VersionedObject import VersionedObject
from snapred.backend.dao.indexing.Versioning import VersionState
from snapred.meta.decorators.Resettable import Resettable
from snapred.ui.view.BackendRequestView import BackendRequestView


@Resettable
class DiffCalSaveView(BackendRequestView):
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

        self.interactionText = QLabel("Assessment Complete! Would you like to save the calibration now?")

        self.fieldRunNumber = self._labeledField("Run Number :")
        self.fieldRunNumber.setEnabled(False)
        self.signalRunNumberUpdate.connect(self._updateRunNumber)

        self.fieldVersion = self._labeledField("Version :")
        # add tooltip to leave blank for new version
        self.fieldVersion.setToolTip("Leave blank for new version!")

        self.fieldAppliesTo = self._labeledField("Applies To :")
        self.fieldAppliesTo.setToolTip(
            "Determines which runs this calibration applies to. 'runNumber', '>runNumber', or \
                '<runNumber', default is '>runNumber'."
        )

        self.fieldComments = self._labeledField("Comments :")
        self.fieldComments.setToolTip("Comments about the calibration, documentation of important information.")

        self.fieldAuthor = self._labeledField("Author :")
        self.fieldAuthor.setToolTip("Author of the calibration.")

        self.iterationDropdown = QComboBox(parent=self)
        self.iterationWidget = self._labeledField("Iteration :", field=self.iterationDropdown)
        self.iterationWidget.setVisible(False)

        _layout = self.layout()
        _layout.addWidget(self.interactionText)
        _layout.addWidget(self.fieldRunNumber)
        _layout.addWidget(self.fieldVersion)
        _layout.addWidget(self.fieldAppliesTo)
        _layout.addWidget(self.fieldComments)
        _layout.addWidget(self.fieldAuthor)

    def validateAndReadForm(self):
        runNumber = self.fieldRunNumber.get()
        version = self.fieldVersion.get(VersionState.NEXT)
        appliesTo = self.fieldAppliesTo.get(f">={runNumber}")
        # validate the version number
        version = VersionedObject.validate_version(version)
        # validate appliesTo field
        appliesTo = IndexEntry.appliesToFormatChecker(appliesTo)
        return runNumber, version, appliesTo, self.fieldComments.get(), self.fieldAuthor.get()

    # This signal boilerplate mumbo jumbo is necessary because worker threads cant update the gui directly
    # So we have to send a signal to the main thread to update the gui, else we get an unhelpful segfault
    @Slot(str)
    def _updateRunNumber(self, runNumber):
        self.fieldRunNumber.setText(runNumber)

    def updateRunNumber(self, runNumber):
        self.signalRunNumberUpdate.emit(runNumber)

    def enableIterationDropdown(self):
        self.iterationWidget.setVisible(True)
        self.layout().addWidget(self.iterationWidget)

    def setIterationDropdown(self, iterations):
        self.resetIterationDropdown()
        self.iterationDropdown.addItems(iterations)
        self.iterationDropdown.setItemText(0, self.currentIterationText)

    def verify(self):
        if self.fieldAuthor.text() == "":
            raise ValueError("You must specify the author")
        if self.fieldComments.text() == "":
            raise ValueError("You must add comments")
        return True

    def setInteractive(self, flag: bool):
        # TODO: put widgets here to allow them to be enabled or disabled by the presenter.
        pass

    def hideIterationDropdown(self):
        self.iterationWidget.setVisible(False)

    def resetIterationDropdown(self):
        self.iterationDropdown.clear()
