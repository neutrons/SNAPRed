import math
import unittest.mock as mock

import matplotlib.pyplot as plt
from mantid.simpleapi import mtd
from PyQt5.QtCore import Qt, pyqtSignal
from PyQt5.QtWidgets import QComboBox, QGridLayout, QHBoxLayout, QLabel, QLineEdit, QPushButton, QSlider, QWidget
from workbench.plotting.figuremanager import FigureManagerWorkbench, MantidFigureCanvas
from workbench.plotting.toolbar import WorkbenchNavigationToolbar

from snapred.meta.Config import Config
from snapred.ui.widget.JsonFormList import JsonFormList
from snapred.ui.widget.LabeledField import LabeledField


class SpecifyNormalizationCalibrationView(QWidget):
    signalRunNumberUpdate = pyqtSignal(str)
    signalGroupingUpdate = pyqtSignal(int)
    signalBackgroundRunNumberUpdate = pyqtSignal(str)
    signalCalibrantUpdate = pyqtSignal(int)
    signalUpdateSmoothingParameter = pyqtSignal(float)
    signalValueChanged = pyqtSignal(int, float, float)
    signalWorkspacesUpdate = pyqtSignal(str, str)
    signalUpdateRecalculationButton = pyqtSignal(bool)
    signalUpdateGraphs = pyqtSignal(bool)

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

        self.sampleDropDown = QComboBox()
        self.sampleDropDown.setEnabled(False)
        self.sampleDropDown.addItems(samples)
        self.sampleDropDown.model().item(0).setEnabled(False)

        self.groupingDropDown = QComboBox()
        self.groupingDropDown.setEnabled(True)
        self.groupingDropDown.addItems(groups)

        self.smoothingSlider = QSlider(Qt.Horizontal)
        self.smoothingSlider.setMinimum(-1000)
        self.smoothingSlider.setMaximum(-20)
        self.smoothingSlider.setValue(-1000)
        self.smoothingSlider.setTickInterval(1)
        self.smoothingSlider.setSingleStep(1)
        self.smoothingSlider.setStyleSheet(
            "QSlider::groove:horizontal {"
            "border: 1px solid #999999;"
            "height: 8px;"
            "background: red;"
            "margin: 2px 0;"
            "}"
            "QSlider::handle:horizontal {"
            "background: white;"
            "border: 1px solid #5c5c5c;"
            "width: 18px;"
            "margin: -2px 0;"
            "border-radius: 3px;"
            "}"
        )

        self.smoothingLineEdit = QLineEdit("1e-9")
        self.smoothingLineEdit.setFixedWidth(50)
        self.smoothingSlider.valueChanged.connect(self.updateLineEditFromSlider)
        self.smoothingLineEdit.returnPressed.connect(
            lambda: self.updateSliderFromLineEdit(self.smoothingLineEdit.text())
        )

        self.fielddMin = LabeledField("dMin :", QLineEdit(str(Config["normalization.parameters.default.dMin"])), self)

        self.recalculationButton = QPushButton("Recalculate")
        self.recalculationButton.clicked.connect(self.emitValueChange)

        smoothingLayout = QHBoxLayout()
        smoothingLayout.addWidget(self.smoothingSlider)
        smoothingLayout.addWidget(self.smoothingLineEdit)
        smoothingLayout.addWidget(self.fielddMin)

        self.layout.addWidget(self.fieldRunNumber, 2, 0)
        self.layout.addWidget(self.fieldBackgroundRunNumber, 2, 1)
        self.layout.addLayout(smoothingLayout, 3, 0)
        self.layout.addWidget(LabeledField("Sample :", self.sampleDropDown, self), 4, 0)
        self.layout.addWidget(LabeledField("Grouping File :", self.groupingDropDown, self), 4, 1)
        self.layout.addWidget(self.recalculationButton, 5, 0, 1, 2)

        self.layout.setRowStretch(1, 3)

        self.signalUpdateRecalculationButton.connect(self.setEnableRecalculateButton)
        self.signalUpdateGraphs.connect(self.updateGraphs)

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

    def updateLineEditFromSlider(self, value):
        v = value / 100.0
        s = 10**v
        self.smoothingLineEdit.setText("{:.2e}".format(s))

    def updateSliderFromLineEdit(self, text):
        try:
            s = float(text)
            v = math.log10(s)
            sliderValue = int(v * 100)
            self.smoothingSlider.setValue(sliderValue)
        except:  # noqa: E722
            raise Exception("Must be a numerical value.")

    def emitValueChange(self):
        index = self.groupingDropDown.currentIndex()
        v = self.smoothingSlider.value() / 100.0
        smoothingValue = 10**v
        dMin = float(self.fielddMin.field.text())
        self.signalValueChanged.emit(index, smoothingValue, dMin)

    def updateWorkspaces(self, focusWorkspace, smoothedWorkspace):
        self.focusWorkspace = focusWorkspace
        self.smoothedWorkspace = smoothedWorkspace
        self.groupingSchema = (
            str(self.groupingDropDown.currentText()).split("/")[-1].split(".")[0].replace("SNAPFocGroup_", "")
        )
        self._updateGraphs()

    def _optimizeRowsAndCols(self, numGraphs):
        # Get best size for layout
        sqrtSize = int(numGraphs**0.5)
        if sqrtSize == numGraphs**0.5:
            rowSize = sqrtSize
            colSize = sqrtSize
        elif numGraphs <= ((sqrtSize + 1) * sqrtSize):
            rowSize = sqrtSize
            colSize = sqrtSize + 1
        else:
            rowSize = sqrtSize + 1
            colSize = sqrtSize + 1
        return rowSize, colSize

    def _updateGraphs(self):
        self.signalUpdateGraphs.emit(True)

    def updateGraphs(self, _):
        self.initFigure()
        self._updateGraphsOption1()

    def initFigure(self):
        self.figure = plt.figure()
        self.canvas = MantidFigureCanvas(self.figure)
        self.navigationBar = WorkbenchNavigationToolbar(self.canvas, self)
        self.layout.addWidget(self.navigationBar, 0, 0)
        self.layout.addWidget(self.canvas, 1, 0, 1, -1)

    def _updateGraphsOption1(self):
        # get the updated workspaces and optimal graph grid
        focusedWorkspace = mtd[self.focusWorkspace]
        smoothedWorkspace = mtd[self.smoothedWorkspace]
        numGraphs = focusedWorkspace.getNumberHistograms()
        nrows, ncols = self._optimizeRowsAndCols(numGraphs)

        # now re-draw the figure
        self.figure.clear()
        for i in range(numGraphs):
            ax = self.figure.add_subplot(nrows, ncols, i + 1, projection="mantid")
            ax.plot(focusedWorkspace, wkspIndex=i, label="Focused Data", normalize_by_bin_width=True)
            ax.plot(smoothedWorkspace, wkspIndex=i, label="Smoothed Data", normalize_by_bin_width=True, linestyle="--")
            ax.legend()
            ax.set_title(f"Group ID: {i + 1}")
            ax.set_xlabel("d-Spacing (Å)")
            ax.set_ylabel("Intensity")
        self.canvas.draw()

    def setEnableRecalculateButton(self, enable):
        self.recalculationButton.setEnabled(enable)

    def disableRecalculateButton(self):
        self.signalUpdateRecalculationButton.emit(False)

    def enableRecalculateButton(self):
        self.signalUpdateRecalculationButton.emit(True)
