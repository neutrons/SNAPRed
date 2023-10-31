from PyQt5.QtCore import pyqtSignal
from PyQt5.QtWidgets import QComboBox, QGridLayout, QLabel, QLineEdit, QWidget

from snapred.ui.widget.JsonFormList import JsonFormList
from snapred.ui.widget.LabeledField import LabeledField


class SpecifyNormalizationCalibrationView(QWidget):
    signalRunNumberUpdate = pyqtSignal(str)
    signalSampleUpdate = pyqtSignal(int)

    def __init__(self, name, jsonSchemaMap, samples=[], parent=None):
        super().__init__(parent)
        self._jsonFormList = JsonFormList(name, jsonSchemaMap, parent=parent)

        self.layout = QGridLayout()
        self.setLayout(self.layout)

        self.interactionText = QLabel(
            "Normalization Calibration Complete! Would you like to assess the calibration now?"
        )

        self.fieldRunNumber = LabeledField("Run Number :", self._jsonFormList.getField("run.runNumber"), self)
        self.fieldRunNumber.setEnabled(False)
        self.signalRunNumberUpdate.connect(self._updateRunNumber)

        self.sampleDropDown = QComboBox()
        self.sampleDropDown.setEnabled(False)
        self.sampleDropDown.addItem("Select Sample")
        self.sampleDropDown.addItems(samples)
        self.sampleDropDown.model().item(0).setEnabled(False)
        self.signalSampleUpdate.connect(self._updateSample)

        self.layout.addWidget(self.interactionText)
        self.layout.addWidget(self.fieldRunNumber)
        self.layout.addWidget(LabeledField("Sample :", self.sampleDropDown, self))

    def _updateRunNumber(self, runNumber):
        self.fieldRunNumber.setText(runNumber)

    def updateRunNumber(self, runNumber):
        self.signalRunNumberUpdate.emit(runNumber)

    def _updateSample(self, sampleIndex):
        self.sampleDropDown.setCurrentIndex(sampleIndex)

    def updateSample(self, sampleIndex):
        self.signalSampleUpdate.emit(sampleIndex)
