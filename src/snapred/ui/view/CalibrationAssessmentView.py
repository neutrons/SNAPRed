from PyQt5.QtCore import pyqtSignal
from PyQt5.QtWidgets import QComboBox, QGridLayout, QLabel, QLineEdit, QWidget

from snapred.meta.decorators.Resettable import Resettable
from snapred.ui.widget.JsonFormList import JsonFormList
from snapred.ui.widget.LabeledField import LabeledField


@Resettable
class CalibrationAssessmentView(QWidget):
    signalRunNumberUpdate = pyqtSignal(str)
    signalSampleUpdate = pyqtSignal(int)

    def __init__(self, name, jsonSchemaMap, samples=[], parent=None):
        super().__init__(parent)
        self._jsonFormList = JsonFormList(name, jsonSchemaMap, parent=parent)

        self.layout = QGridLayout()
        self.setLayout(self.layout)

        self.interactionText = QLabel("Calibration Complete! Would you like to assess the calibration now?")

        self.fieldRunNumber = LabeledField("Run Number :", QLineEdit(parent=self), self)
        self.fieldRunNumber.setEnabled(False)
        self.signalRunNumberUpdate.connect(self._updateRunNumber)

        self.sampleDropdown = QComboBox()
        self.sampleDropdown.setEnabled(False)
        self.sampleDropdown.addItem("Select Sample")
        self.sampleDropdown.addItems(samples)
        self.sampleDropdown.model().item(0).setEnabled(False)
        self.signalSampleUpdate.connect(self._updateSample)

        self.layout.addWidget(self.interactionText)
        self.layout.addWidget(self.fieldRunNumber)
        self.layout.addWidget(LabeledField("Sample :", self.sampleDropdown, self))

    # This signal boilerplate mumbo jumbo is necessary because worker threads cant update the gui directly
    # So we have to send a signal to the main thread to update the gui, else we get an unhelpful segfault
    def _updateRunNumber(self, runNumber):
        self.fieldRunNumber.setText(runNumber)

    def updateRunNumber(self, runNumber):
        self.signalRunNumberUpdate.emit(runNumber)

    def _updateSample(self, sampleIndex):
        self.sampleDropdown.setCurrentIndex(sampleIndex)

    def updateSample(self, sampleIndex):
        self.signalSampleUpdate.emit(sampleIndex)
