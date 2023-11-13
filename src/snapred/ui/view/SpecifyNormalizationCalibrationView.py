from PyQt5.QtCore import pyqtSignal
from PyQt5.QtWidgets import QComboBox, QGridLayout, QLabel, QLineEdit, QWidget

from snapred.ui.widget.JsonFormList import JsonFormList
from snapred.ui.widget.LabeledField import LabeledField


class SpecifyNormalizationCalibrationView(QWidget):
    signalRunNumberUpdate = pyqtSignal(str)
    signalSampleUpdate = pyqtSignal(int)
    signalGroupingUpdate = pyqtSignal(int)
    signalBackgroundRunNumberUpdate = pyqtSignal(str)
    signalCalibrantUpdate = pyqtSignal(int)
    signalSmoothingParameterUpdate = pyqtSignal(float)

    def __init__(
        self, name, jsonSchemaMap, smoothingParameter, samples=[], groups=[], calibrantSamples=[], parent=None
    ):
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

        self.fieldBackgroundRunNumber = LabeledField(
            "Background Run Number :", self._jsonFormList.getField("run.backgroundRunNumber"), self
        )
        self.fieldBackgroundRunNumber.setEnabled(False)
        self.signalBackgroundRunNumberUpdate.connect(self._updateBackgroundRunNumber)

        self.smoothingParameter = LabeledField("Smoothing Parameter :", smoothingParameter, self)
        self.smoothingParameter.setEnabled(False)
        self.signalSmoothingParameterUpdate.connect(self._updateSmoothingParameter)

        self.sampleDropDown = QComboBox()
        self.sampleDropDown.setEnabled(False)
        self.sampleDropDown.addItem("Select Sample")
        self.sampleDropDown.addItems(samples)
        self.sampleDropDown.model().item(0).setEnabled(False)
        self.signalSampleUpdate.connect(self._updateSample)

        self.groupingDropDown = QComboBox()
        self.groupingDropDown.setEnabled(False)
        self.groupingDropDown.addItem("Select Grouping")
        self.groupingDropDown.addItems(groups)
        self.groupingDropDown.model().item(0).setEnabled(False)
        self.signalGroupingUpdate.connect(self._updateGrouping)

        self.calibrantDropDown = QComboBox()
        self.calibrantDropDown.setEnabled(False)
        self.calibrantDropDown.addItem("Select Calibrant")
        self.calibrantDropDown.addItems(calibrantSamples)
        self.calibrantDropDown.model().item(0).setEnabled(False)
        self.calibrantDropDown.connect(self._updateCalibrant)

        self.layout.addWidget(self.interactionText)
        self.layout.addWidget(self.fieldRunNumber)
        self.layout.addWidget(self.fieldBackgroundRunNumber)
        self.layout.addWidget(self.smoothingParameter)
        self.layout.addWidget(LabeledField("Sample :", self.sampleDropDown, self))
        self.layout.addWidget(LabeledField("Grouping File :", self.groupingDropDown, self))
        self.layout.addWidget(LabeledField("Calibrant :", self.calibrantDropDown, self))

    def _updateRunNumber(self, runNumber):
        self.fieldRunNumber.setText(runNumber)

    def updateRunNumber(self, runNumber):
        self.signalRunNumberUpdate.emit(runNumber)

    def _updateBackgroundRunNumber(self, backgroundRunNumber):
        self.fieldBackgroundRunNumber.setText(backgroundRunNumber)

    def updateBackgroundRunNumber(self, backgroundRunNumber):
        self.signalBackgroundRunNumberUpdate.emit(backgroundRunNumber)

    def _updateSample(self, sampleIndex):
        self.sampleDropDown.setCurrentIndex(sampleIndex)

    def updateSample(self, sampleIndex):
        self.signalSampleUpdate.emit(sampleIndex)

    def _updateGrouping(self, groupingIndex):
        self.groupingDropDown.setCurrentIndex(groupingIndex)

    def updateGrouping(self, groupingIndex):
        self.signalGroupingUpdate.emit(groupingIndex)

    def _updateCalibrant(self, calibrantIndex):
        self.calibrantDropDown.setCurrentIndex(calibrantIndex)

    def updateCalibrant(self, calibrantIndex):
        self.signalCalibrantUpdate.emit(calibrantIndex)

    def _updateSmoothingParameter(self, smoothingParameter):
        self.smoothingParameter.setText(smoothingParameter)

    def updateSmoothingParameter(self, smoothingParameter):
        self.signalSmoothingParameterUpdate.emit(smoothingParameter)
