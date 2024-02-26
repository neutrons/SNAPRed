import math
import unittest.mock as mock
from typing import List

import matplotlib.pyplot as plt
from mantid.plots.datafunctions import get_spectrum
from mantid.simpleapi import mtd
from pydantic import parse_obj_as
from PyQt5.QtCore import Qt, pyqtSignal
from PyQt5.QtWidgets import (
    QComboBox,
    QGridLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPushButton,
    QSlider,
    QVBoxLayout,
    QWidget,
)
from workbench.plotting.figuremanager import FigureManagerWorkbench, MantidFigureCanvas
from workbench.plotting.toolbar import WorkbenchNavigationToolbar

from snapred.backend.dao import GroupPeakList
from snapred.meta.Config import Config
from snapred.meta.decorators.Resettable import Resettable
from snapred.ui.widget.JsonFormList import JsonFormList
from snapred.ui.widget.LabeledField import LabeledField


@Resettable
class SpecifyNormalizationCalibrationView(QWidget):
    signalRunNumberUpdate = pyqtSignal(str)
    signalBackgroundRunNumberUpdate = pyqtSignal(str)
    signalValueChanged = pyqtSignal(int, float, float, float, float)
    signalUpdateRecalculationButton = pyqtSignal(bool)

    def __init__(self, name, jsonSchemaMap, samples=[], groups=[], parent=None):
        super().__init__(parent)
        self._jsonFormList = JsonFormList(name, jsonSchemaMap, parent=parent)

        self.layout = QGridLayout()
        self.setLayout(self.layout)

        # create the run number fields
        self.fieldRunNumber = LabeledField("Run Number :", self._jsonFormList.getField("run.runNumber"), self)
        self.fieldRunNumber.setEnabled(False)
        self.signalRunNumberUpdate.connect(self._updateRunNumber)

        self.fieldBackgroundRunNumber = LabeledField(
            "Background Run Number :", self._jsonFormList.getField("run.backgroundRunNumber"), self
        )
        self.fieldBackgroundRunNumber.setEnabled(False)
        self.signalBackgroundRunNumberUpdate.connect(self._updateBackgroundRunNumber)

        # create the graph elements
        self.figure = plt.figure(constrained_layout=True)
        self.canvas = MantidFigureCanvas(self.figure)
        self.navigationBar = WorkbenchNavigationToolbar(self.canvas, self)

        # create the other specification elements
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
        self.smoothingLineEdit.setMinimumWidth(128)
        self.smoothingSlider.valueChanged.connect(self.updateLineEditFromSlider)
        self.smoothingLineEdit.returnPressed.connect(
            lambda: self.updateSliderFromLineEdit(self.smoothingLineEdit.text())
        )
        self.smoothingLabel = QLabel("Smoothing :")

        self.fielddMin = LabeledField("dMin :", QLineEdit(str(Config["constants.CrystallographicInfo.dMin"])), self)
        self.fielddMax = LabeledField("dMax :", QLineEdit(str(Config["constants.CrystallographicInfo.dMax"])), self)
        self.fieldThreshold = LabeledField(
            "intensity threshold :", QLineEdit(str(Config["constants.PeakIntensityFractionThreshold"])), self
        )

        self.recalculationButton = QPushButton("Recalculate")
        self.recalculationButton.clicked.connect(self.emitValueChange)

        smoothingLayout = QHBoxLayout()
        smoothingLayout.addWidget(self.smoothingLabel)
        smoothingLayout.addWidget(self.smoothingSlider)
        smoothingLayout.addWidget(self.smoothingLineEdit)
        smoothingLayout.addWidget(self.fieldThreshold)
        smoothingLayout.addWidget(self.fielddMin)
        smoothingLayout.addWidget(self.fielddMax)

        # self.fieldLayout = QGridLayout()

        # add all elements to the grid layout
        self.layout.addWidget(self.navigationBar, 0, 0)
        self.layout.addWidget(self.canvas, 1, 0, 1, -1)
        self.layout.addWidget(self.fieldRunNumber, 2, 0)
        self.layout.addWidget(self.fieldBackgroundRunNumber, 2, 1)
        self.layout.addLayout(smoothingLayout, 3, 0, 1, 2)
        self.layout.addWidget(LabeledField("Sample :", self.sampleDropDown, self), 4, 0)
        self.layout.addWidget(LabeledField("Grouping File :", self.groupingDropDown, self), 4, 1)
        self.layout.addWidget(self.recalculationButton, 5, 0, 1, 2)

        self.layout.setRowStretch(1, 3)

        # store the initial layout without graphs
        self.initialLayoutHeight = self.size().height()

        self.signalUpdateRecalculationButton.connect(self.setEnableRecalculateButton)

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
        dMax = float(self.fielddMax.field.text())
        peakThreshold = float(self.fieldThreshold.text())
        if dMin < 0.1:
            response = QMessageBox.warning(
                self,
                "Warning!!!",
                "Are you sure you want to do this? This may cause memory overflow or may take a long time to compute.",
                QMessageBox.Yes | QMessageBox.No,
            )
            if response == QMessageBox.No:
                return
        elif dMin > dMax:
            QMessageBox.warning(
                self,
                "Warning!!!",
                f"The dMin value exceeds the allowed maximum dMax value ({dMax}). Please enter a smaller value.",
                QMessageBox.Ok,
            )
            return
        self.signalValueChanged.emit(index, smoothingValue, dMin, dMax, peakThreshold)

    def updateWorkspaces(self, focusWorkspace, smoothedWorkspace, peaks):
        self.focusWorkspace = focusWorkspace
        self.smoothedWorkspace = smoothedWorkspace
        self.groupingSchema = (
            str(self.groupingDropDown.currentText()).split("/")[-1].split(".")[0].replace("SNAPFocGroup_", "")
        )
        self._updateGraphs(peaks)

    def _updateGraphs(self, peaks):
        # get the updated workspaces and optimal graph grid
        focusedWorkspace = mtd[self.focusWorkspace]
        smoothedWorkspace = mtd[self.smoothedWorkspace]
        peaks = parse_obj_as(List[GroupPeakList], peaks)
        numGraphs = focusedWorkspace.getNumberHistograms()
        nrows, ncols = self._optimizeRowsAndCols(numGraphs)

        # now re-draw the figure
        self.figure.clear()
        for i in range(numGraphs):
            ax = self.figure.add_subplot(nrows, ncols, i + 1, projection="mantid")
            ax.plot(focusedWorkspace, wkspIndex=i, label="Focused Data", normalize_by_bin_width=True)
            ax.plot(smoothedWorkspace, wkspIndex=i, label="Smoothed Data", normalize_by_bin_width=True, linestyle="--")
            ax.legend()
            ax.tick_params(direction="in")
            ax.set_title(f"Group ID: {i + 1}")
            ax.set_xlabel("d-Spacing (Å)")
            ax.set_ylabel("Intensity")
            # fill in the discovered peaks for easier viewing
            x, y, _, _ = get_spectrum(focusedWorkspace, i, normalize_by_bin_width=True)
            # for each detected peak in this group, shade in the peak region
            for peak in peaks[i].peaks:
                under_peaks = [(peak.minimum < xx and xx < peak.maximum) for xx in x]
                ax.fill_between(x, y, where=under_peaks, color="blue", alpha=0.5)
            # plot the min value for peaks
            ax.axvline(x=max(min(x), float(self.fielddMin.field.text())), label="dMin", color="red")
            ax.axvline(x=min(max(x), float(self.fielddMax.field.text())), label="dMax", color="red")

        # resize window and redraw
        self.setMinimumHeight(self.initialLayoutHeight + int(self.figure.get_size_inches()[1] * self.figure.dpi))
        self.canvas.draw()

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

    def setEnableRecalculateButton(self, enable):
        self.recalculationButton.setEnabled(enable)

    def disableRecalculateButton(self):
        self.signalUpdateRecalculationButton.emit(False)

    def enableRecalculateButton(self):
        self.signalUpdateRecalculationButton.emit(True)
