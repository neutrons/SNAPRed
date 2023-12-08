import matplotlib
import matplotlib.pyplot as plt
import numpy as np
from mantid.simpleapi import mtd
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from PyQt5.QtCore import Qt, pyqtSignal
from PyQt5.QtWidgets import QComboBox, QGridLayout, QLabel, QLineEdit, QSlider, QWidget

from snapred.ui.widget.JsonFormList import JsonFormList
from snapred.ui.widget.LabeledField import LabeledField


class SpecifyNormalizationCalibrationView(QWidget):
    signalRunNumberUpdate = pyqtSignal(str)
    signalGroupingUpdate = pyqtSignal(int)
    signalBackgroundRunNumberUpdate = pyqtSignal(str)
    signalCalibrantUpdate = pyqtSignal(int)
    signalUpdateSmoothingParameter = pyqtSignal(float)
    signalValueChanged = pyqtSignal(int, float)
    signalWorkspacesUpdate = pyqtSignal(str, str)

    def __init__(self, name, jsonSchemaMap, samples=[], groups=[], parent=None):
        super().__init__(parent)
        self._jsonFormList = JsonFormList(name, jsonSchemaMap, parent=parent)

        self.groupingSchema = None
        self.subplots = []

        self.layout = QGridLayout()
        self.setLayout(self.layout)

        self.fieldRunNumber = LabeledField("Run Number :", self._jsonFormList.getField("run.runNumber"), self)
        self.fieldRunNumber.setEnabled(False)
        self.signalRunNumberUpdate.connect(self._updateRunNumber)

        self.fieldBackgroundRunNumber = LabeledField(
            "Background Run Number :", self._jsonFormList.getField("run.backgroundRunNumber"), self
        )
        self.fieldBackgroundRunNumber.setEnabled(False)
        self.signalBackgroundRunNumberUpdate.connect(self._updateBackgroundRunNumber)

        self.figure = plt.figure()
        self.canvas = FigureCanvas(self.figure)

        self.sampleDropDown = QComboBox()
        self.sampleDropDown.setEnabled(False)
        self.sampleDropDown.addItems(samples)
        self.sampleDropDown.model().item(0).setEnabled(False)

        self.groupingDropDown = QComboBox()
        self.groupingDropDown.setEnabled(True)
        self.groupingDropDown.addItems(groups)
        self.groupingDropDown.model().item(0).setEnabled(False)
        self.signalGroupingUpdate.connect(self._updateGrouping)
        self.groupingDropDown.currentIndexChanged.connect(self.onValueChanged)

        self.smoothingSlider = QSlider(Qt.Horizontal)
        self.smoothingSlider.setMinimum(0)
        self.smoothingSlider.setMaximum(100)
        self.smoothingSlider.setValue(0)
        self.smoothingSlider.setTickInterval(1)
        self.smoothingSlider.setSingleStep(1)
        self.smoothingSlider.valueChanged.connect(self.onValueChanged)

        self.layout.addWidget(self.canvas, 0, 0, 1, -1)
        self.layout.addWidget(self.fieldRunNumber, 1, 0)
        self.layout.addWidget(self.fieldBackgroundRunNumber, 1, 1)
        self.layout.addWidget(LabeledField("Smoothing Parameter:", self.smoothingSlider, self), 1, 2)
        self.layout.addWidget(LabeledField("Sample :", self.sampleDropDown, self), 1, 3)
        self.layout.addWidget(LabeledField("Grouping File :", self.groupingDropDown, self), 1, 4)

        self.layout.setRowStretch(0, 3)
        self.layout.setRowStretch(1, 1)

    def _updateRunNumber(self, runNumber):
        self.fieldRunNumber.setText(runNumber)

    def updateRunNumber(self, runNumber):
        self.signalRunNumberUpdate.emit(runNumber)

    def _updateBackgroundRunNumber(self, backgroundRunNumber):
        self.fieldBackgroundRunNumber.setText(backgroundRunNumber)

    def updateBackgroundRunNumber(self, backgroundRunNumber):
        self.signalBackgroundRunNumberUpdate.emit(backgroundRunNumber)

    def updateFields(self, sampleIndex, groupingIndex, smoothingParameter):
        self.sampleDropDown.setCurrentIndex(sampleIndex)
        self.groupingDropDown.setCurrentIndex(groupingIndex)
        self.smoothingSlider.setValue(int(smoothingParameter * 100))

    def _updateGrouping(self):
        self.signalGroupingUpdate.emit(self.groupingDropDown.currentIndex())

    # def updateSample(self, sampleIndex):
    #     self.signalSampleUpdate.emit(sampleIndex)

    # def updateGrouping(self, groupingIndex):
    #     self.signalGroupingUpdate.emit(groupingIndex)

    # def _updateSmoothingParameter(self, smoothingParameter):
    #     self.smoothingSlider.setValue(smoothingParameter * 100)

    # def updateSmoothingParameter(self, smoothingParameter):
    #     self.signalUpdateSmoothingParameter.emit(smoothingParameter)

    def onValueChanged(self):
        index = self.groupingDropDown.currentIndex()
        smoothingValue = (self.smoothingSlider.value()) / 100.0
        self.signalValueChanged.emit(index, smoothingValue)

    def updateWorkspaces(self, focusWorkspace, smoothedWorkspace):
        self.focusWorkspace = focusWorkspace
        self.smoothedWorkspace = smoothedWorkspace
        self.groupingSchema = (
            str(self.groupingDropDown.currentText()).split("/")[-1].split(".")[0].replace("SNAPFocGroup_", "")
        )
        self._updateGraphs()

    def _updateGraphs(self):
        self.figure.clear()
        self.subplots.clear()

        if self.groupingSchema == "All":
            numGraphs = 1
        elif self.groupingSchema == "Bank":
            numGraphs = 2
        elif self.groupingSchema == "Column":
            numGraphs = 6
        else:
            raise Exception("Invalid grouping schema or this schema is not yet supported.")

        for i in range(numGraphs * 2):
            ax = self.figure.add_subplot(2, numGraphs, i + 1)
            self.subplots.append(ax)

        self._updatePlots(numGraphs)

    def _updatePlots(self, numGraphs):
        for i, ax in enumerate(self.subplots):
            groupIndex = i // numGraphs

            focusedWorkspace = mtd[self.focusWorkspace]
            smoothedWorkspace = mtd[self.smoothedWorkspace]

            focusedData = focusedWorkspace.readY(groupIndex)
            smoothedData = smoothedWorkspace.readY(groupIndex)

            ax.plot(focusedData, label="Focused Data")
            ax.plot(smoothedData, label="Smoothed Data", linestyle="--")

            ax.legend()

        self.canvas.draw()
