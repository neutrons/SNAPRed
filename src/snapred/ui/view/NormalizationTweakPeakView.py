from typing import List

import matplotlib.pyplot as plt
from mantid.plots.datafunctions import get_spectrum
from mantid.simpleapi import mtd
from pydantic import parse_obj_as
from qtpy.QtCore import Signal
from qtpy.QtWidgets import (
    QHBoxLayout,
    QLineEdit,
    QMessageBox,
    QPushButton,
)
from workbench.plotting.figuremanager import MantidFigureCanvas
from workbench.plotting.toolbar import WorkbenchNavigationToolbar

from snapred.backend.dao import GroupPeakList
from snapred.meta.Config import Config
from snapred.meta.decorators.Resettable import Resettable
from snapred.ui.view.BackendRequestView import BackendRequestView
from snapred.ui.widget.SmoothingSlider import SmoothingSlider


@Resettable
class NormalizationTweakPeakView(BackendRequestView):
    """

    This PyQt5 GUI component is designed for adjusting peak normalization parameters in SNAPRed,
    offering a user-friendly interface that combines input fields, dropdowns, sliders, and a
    real-time matplotlib plot area. It is built for dynamic interaction and visualization, allowing
    users to see the impact of their adjustments on the normalization settings instantly. Key
    features include a configurable layout with signal-slot connections for real-time updates,
    matplotlib integration for data plotting, and various controls for precise parameter tuning.
    This setup not only facilitates an interactive adjustment process but also provides immediate
    visual feedback and validation, significantly improving the user experience in optimizing
    normalization parameters for data analysis.

    """

    DMIN = Config["constants.CrystallographicInfo.dMin"]
    DMAX = Config["constants.CrystallographicInfo.dMax"]
    PEAK_THRESHOLD = Config["constants.PeakIntensityFractionThreshold"]

    signalRunNumberUpdate = Signal(str)
    signalBackgroundRunNumberUpdate = Signal(str)
    signalValueChanged = Signal(int, float, float, float, float)
    signalUpdateRecalculationButton = Signal(bool)
    signalUpdateFields = Signal(int, int, float)
    signalPopulateGroupingDropdown = Signal(list)

    def __init__(self, jsonForm, samples=[], groups=[], parent=None):
        selection = ""
        super().__init__(jsonForm, selection, parent=parent)

        # create the run number fields
        self.fieldRunNumber = self._labeledField("Run Number", QLineEdit(parent=self))
        self.fieldBackgroundRunNumber = self._labeledField("Background Run Number", QLineEdit(parent=self))
        # connect them to signals
        self.signalRunNumberUpdate.connect(self._updateRunNumber)
        self.signalBackgroundRunNumberUpdate.connect(self._updateBackgroundRunNumber)

        # create the graph elements
        self.figure = plt.figure(constrained_layout=True)
        self.canvas = MantidFigureCanvas(self.figure)
        self.navigationBar = WorkbenchNavigationToolbar(self.canvas, self)

        # create the other specification elements
        self.sampleDropdown = self._sampleDropDown("Sample", samples)
        self.groupingFileDropdown = self._sampleDropDown("Grouping File", groups)

        # disable run number, background, sample -- cannot be changed now
        for x in [self.fieldRunNumber, self.fieldBackgroundRunNumber, self.sampleDropdown]:
            x.setEnabled(False)

        # create the adjustment controls
        self.smoothingSlider = self._labeledField("Smoothing", SmoothingSlider())
        self.fielddMin = self._labeledField("dMin", QLineEdit(str(self.DMIN)))
        self.fielddMax = self._labeledField("dMax", QLineEdit(str(self.DMAX)))
        self.fieldThreshold = self._labeledField("intensity threshold", QLineEdit(str(self.PEAK_THRESHOLD)))
        peakControlLayout = QHBoxLayout()
        peakControlLayout.addWidget(self.smoothingSlider, 2)
        peakControlLayout.addWidget(self.fielddMin)
        peakControlLayout.addWidget(self.fielddMax)
        peakControlLayout.addWidget(self.fieldThreshold)

        # a big ol recalculate button
        self.recalculationButton = QPushButton("Recalculate")
        self.recalculationButton.clicked.connect(self.emitValueChange)

        # add all elements to the grid layout
        self.layout.addWidget(self.navigationBar, 0, 0)
        self.layout.addWidget(self.canvas, 1, 0, 1, -1)
        self.layout.addWidget(self.fieldRunNumber, 2, 0)
        self.layout.addWidget(self.fieldBackgroundRunNumber, 2, 1)
        self.layout.addLayout(peakControlLayout, 3, 0, 1, 2)
        self.layout.addWidget(self.sampleDropdown, 4, 0)
        self.layout.addWidget(self.groupingFileDropdown, 4, 1)
        self.layout.addWidget(self.recalculationButton, 5, 0, 1, 2)

        self.layout.setRowStretch(1, 10)

        # store the initial layout without graphs
        self.initialLayoutHeight = self.size().height()

        self.signalUpdateRecalculationButton.connect(self.setEnableRecalculateButton)
        self.signalUpdateFields.connect(self._updateFields)
        self.signalPopulateGroupingDropdown.connect(self._populateGroupingDropdown)

    def _updateRunNumber(self, runNumber):
        self.fieldRunNumber.setText(runNumber)

    def updateRunNumber(self, runNumber):
        self.signalRunNumberUpdate.emit(runNumber)

    def _updateBackgroundRunNumber(self, backgroundRunNumber):
        self.fieldBackgroundRunNumber.setText(backgroundRunNumber)

    def updateBackgroundRunNumber(self, backgroundRunNumber):
        self.signalBackgroundRunNumberUpdate.emit(backgroundRunNumber)

    def _updateFields(self, sampleIndex, groupingIndex, smoothingParameter):
        self.sampleDropdown.setCurrentIndex(sampleIndex)
        self.groupingFileDropdown.setCurrentIndex(groupingIndex)
        self.smoothingSlider.field.setValue(smoothingParameter)

    def updateFields(self, sampleIndex, groupingIndex, smoothingParameter):
        self.signalUpdateFields.emit(sampleIndex, groupingIndex, smoothingParameter)

    def emitValueChange(self):
        # verify the fields before recalculation
        try:
            index = self.groupingFileDropdown.currentIndex()
            smoothingValue = self.smoothingSlider.field.value()
            dMin = float(self.fielddMin.field.text())
            dMax = float(self.fielddMax.field.text())
            peakThreshold = float(self.fieldThreshold.text())
        except ValueError as e:
            QMessageBox.warning(
                self,
                "Invalid Peak Parameters",
                f"One of dMin, dMax, smoothing, or peak threshold is invalid: {str(e)}",
                QMessageBox.Ok,
            )
            return
        # perform some checks on dMin, dMax values
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
        self.groupingSchema = self.groupingFileDropdown.currentText()
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

    def _populateGroupingDropdown(self, groups=["Enter a Run Number"]):
        self.groupingFileDropdown.setItems(groups)
        self.groupingFileDropdown.setEnabled(True)

    def populateGroupingDropdown(self, groups=["Enter a Run Number"]):
        self.signalPopulateGroupingDropdown.emit(groups)

    def verify(self):
        # TODO what needs to be verified?
        return True
