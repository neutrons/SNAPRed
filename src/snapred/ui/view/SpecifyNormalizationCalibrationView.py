import matplotlib.pyplot as plt
from mantid.simpleapi import mtd
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from PyQt5.QtCore import Qt, pyqtSignal
from PyQt5.QtWidgets import QComboBox, QGridLayout, QHBoxLayout, QLabel, QSlider, QWidget

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
        self.groupingDropDown.currentIndexChanged.connect(self.onValueChanged)

        self.smoothingSlider = QSlider(Qt.Horizontal)
        self.smoothingSlider.setMinimum(0)
        self.smoothingSlider.setMaximum(100)
        self.smoothingSlider.setValue(0)
        self.smoothingSlider.setTickInterval(1)
        self.smoothingSlider.setSingleStep(1)
        self.smoothingSlider.valueChanged.connect(self.onValueChanged)

        # Create a layout for the smoothing parameter label and slider
        smoothingLayout = QHBoxLayout()
        self.smoothingValueLabel = QLabel("0.00")
        self.smoothingValueLabel.setAlignment(Qt.AlignCenter)
        smoothingLayout.addWidget(self.smoothingSlider)
        smoothingLayout.addWidget(self.smoothingValueLabel)

        self.layout.addWidget(self.canvas, 0, 0, 1, -1)
        self.layout.addWidget(self.fieldRunNumber, 1, 0)
        self.layout.addWidget(self.fieldBackgroundRunNumber, 1, 1)
        self.layout.addLayout(smoothingLayout, 2, 0)
        self.layout.addWidget(LabeledField("Sample :", self.sampleDropDown, self), 3, 0)
        self.layout.addWidget(LabeledField("Grouping File :", self.groupingDropDown, self), 3, 1)

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

    def onValueChanged(self):
        index = self.groupingDropDown.currentIndex()
        smoothingValue = (self.smoothingSlider.value()) / 100.0
        self.smoothingValueLabel.setText("{:.2f}".format(smoothingValue))
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
        self.figure.clear()
        self.subplots = []

        for i in range(numGraphs):
            ax = self.figure.add_subplot(1, numGraphs, i + 1)
            self.subplots.append(ax)

        focusedWorkspace = mtd[self.focusWorkspace]
        smoothedWorkspace = mtd[self.smoothedWorkspace]
        for i, ax in enumerate(self.subplots):
            if i < focusedWorkspace.getNumberHistograms():
                focusedData = focusedWorkspace.readY(i)
                smoothedData = smoothedWorkspace.readY(i)

                ax.plot(focusedData, label="Focused Data")
                ax.plot(smoothedData, label="Smoothed Data", linestyle="--")
                ax.legend()
                ax.set_title(f"Group ID: {i + 1}")

                ax.set_xlabel("d-Spacing (Å)")
                ax.set_ylabel("Intensity")

        self.figure.tight_layout()

        self.canvas.draw()
