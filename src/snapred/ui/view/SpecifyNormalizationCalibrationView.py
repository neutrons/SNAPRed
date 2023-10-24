from PyQt5.QtCore import pyqtSignal
from PyQt5.QtWidgets import QComboBox, QGridLayout, QLabel, QLineEdit, QWidget

from snapred.ui.widget.JsonFormList import JsonFormList
from snapred.ui.widget.LabeledField import LabeledField


class SpecifyNormalizationCalibrationView(QWidget):
    signalRunNumberUpdate = pyqtSignal(str)
    signalCalibrantUpdate = pyqtSignal(int)

    def __init__(self, name, jsonSchemaMap, samples=[], parent=None):
        super().__init__(parent)
        self._jsonFormList = JsonFormList(name, jsonSchemaMap, parent=parent)

        self.layout = QGridLayout()
        self.setLayout(self.layout)

        self.interactionText = QLabel("Normalization Calibration Complete! Would you like to assess the calibration now?")

        self.fieldRunNumber = LabeledField("Run Number :", self._jsonFormList.getField("run.runNumber"), self)
        self.fieldRunNumber.setEnabled(False)
        self.signalRunNumberUpdate.connect(self._updateRunNumber)

        self.calibrantDropDown = QComboBox()
        self.calibrantDropDown.setEnabled(False)
        self.calibrantDropDown.addItem("Select Calibrant")
        self.calibrantDropDown.addItems(samples)
        self.calibrantDropDown.model().item(0).setEnabled(False)
        self.signalCalibrantUpdate.connect(self._updateCalibrant)

        self.layout.addWidget(self.interactionText)
        self.layout.addWidget(self.fieldRunNumber)
        self.layout.addWidget(LabeledField("Sample :", self.calibrantDropDown, self))

    def _updateRunNumber(self, runNumber):
        self.fieldRunNumber.setText(runNumber)

    def updateRunNumber(self, runNumber):
        self.signalRunNumberUpdate.emit(runNumber)

    def _updateCalibrant(self, calibrantIndex):
        self.calibrantDropDown.setCurrentIndex(calibrantIndex)

    def updateCalibrant(self, calibrantIndex):
        self.signalCalibrantUpdate.emit(calibrantIndex)
