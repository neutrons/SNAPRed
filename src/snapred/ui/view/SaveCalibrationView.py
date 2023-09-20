from PyQt5.QtCore import pyqtSignal
from PyQt5.QtWidgets import QGridLayout, QLabel, QWidget

from snapred.ui.widget.JsonFormList import JsonFormList
from snapred.ui.widget.LabeledField import LabeledField


class SaveCalibrationView(QWidget):
    signalRunNumberUpdate = pyqtSignal(str)

    def __init__(self, name, jsonSchemaMap, parent=None):
        super().__init__(parent)
        self._jsonFormList = JsonFormList(name, jsonSchemaMap, parent=parent)

        self.layout = QGridLayout()
        self.setLayout(self.layout)

        self.interactionText = QLabel("Assessment Complete! Would you like to save the calibration now?")

        self.fieldRunNumber = LabeledField(
            "Run Number :", self._jsonFormList.getField("calibrationIndexEntry.runNumber"), self
        )
        self.fieldRunNumber.setEnabled(False)
        self.signalRunNumberUpdate.connect(self._updateRunNumber)

        self.fieldVersion = LabeledField(
            "Version :", self._jsonFormList.getField("calibrationIndexEntry.version"), self
        )
        # add tooltip to leave blank for new version
        self.fieldVersion.setToolTip("Leave blank for new version!")

        self.fieldAppliesTo = LabeledField(
            "Applies To :", self._jsonFormList.getField("calibrationIndexEntry.appliesTo"), self
        )
        self.fieldAppliesTo.setToolTip(
            "Determines which runs this calibration applies to. 'runNumber', '>runNumber', or \
                '<runNumber', default is '>runNumber'."
        )

        self.fieldComments = LabeledField(
            "Comments :", self._jsonFormList.getField("calibrationIndexEntry.comments"), self
        )
        self.fieldComments.setToolTip("Comments about the calibration, documentation of important information.")

        self.fieldAuthor = LabeledField("Author :", self._jsonFormList.getField("calibrationIndexEntry.author"), self)
        self.fieldAuthor.setToolTip("Author of the calibration.")

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
