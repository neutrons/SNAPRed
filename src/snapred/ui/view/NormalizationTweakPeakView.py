from typing import List

import matplotlib.pyplot as plt
import pydantic
from mantid.plots.datafunctions import get_spectrum
from mantid.simpleapi import mtd
from qtpy.QtCore import Signal, Slot
from qtpy.QtWidgets import (
    QHBoxLayout,
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

    This qt GUI component is designed for adjusting peak normalization parameters in SNAPRed,
    offering a user-friendly interface that combines input fields, dropdowns, sliders, and a
    real-time matplotlib plot area. It is built for dynamic interaction and visualization, allowing
    users to see the impact of their adjustments on the normalization settings instantly. Key
    features include a configurable layout with signal-slot connections for real-time updates,
    matplotlib integration for data plotting, and various controls for precise parameter tuning.
    This setup not only facilitates an interactive adjustment process but also provides immediate
    visual feedback and validation, significantly improving the user experience in optimizing
    normalization parameters for data analysis.

    """

    XTAL_DMIN = Config["constants.CrystallographicInfo.crystalDMin"]
    XTAL_DMAX = Config["constants.CrystallographicInfo.crystalDMax"]

    signalRunNumberUpdate = Signal(str)
    signalBackgroundRunNumberUpdate = Signal(str)
    signalValueChanged = Signal(int, float, float, float)
    signalUpdateRecalculationButton = Signal(bool)
    signalUpdateFields = Signal(int, int, float)
    signalPopulateGroupingDropdown = Signal(list)

    def __init__(self, samples=[], groups=[], parent=None):
        super().__init__(parent=parent)

        # create the run number fields
        self.fieldRunNumber = self._labeledField("Run Number")
        self.fieldBackgroundRunNumber = self._labeledField("Background Run Number")
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
        self.fieldXtalDMin = self._labeledField("xtal dMin", text=str(self.XTAL_DMIN))
        self.fieldXtalDMax = self._labeledField("xtal dMax", text=str(self.XTAL_DMAX))
        peakControlLayout = QHBoxLayout()
        peakControlLayout.addWidget(self.smoothingSlider, 2)
        peakControlLayout.addWidget(self.fieldXtalDMin)
        peakControlLayout.addWidget(self.fieldXtalDMax)

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

    @Slot(str)
    def _updateRunNumber(self, runNumber):
        self.fieldRunNumber.setText(runNumber)

    def updateRunNumber(self, runNumber):
        self.signalRunNumberUpdate.emit(runNumber)

    @Slot(str)
    def _updateBackgroundRunNumber(self, backgroundRunNumber):
        self.fieldBackgroundRunNumber.setText(backgroundRunNumber)

    def updateBackgroundRunNumber(self, backgroundRunNumber):
        self.signalBackgroundRunNumberUpdate.emit(backgroundRunNumber)

    @Slot(int, int, float)
    def _updateFields(self, sampleIndex, groupingIndex, smoothingParameter):
        self.sampleDropdown.setCurrentIndex(sampleIndex)
        self.groupingFileDropdown.setCurrentIndex(groupingIndex)
        self.smoothingSlider.field.setValue(smoothingParameter)

    def updateFields(self, sampleIndex, groupingIndex, smoothingParameter):
        self.signalUpdateFields.emit(sampleIndex, groupingIndex, smoothingParameter)

    @Slot()
    def emitValueChange(self):
        # verify the fields before recalculation
        try:
            index = self.groupingFileDropdown.currentIndex()
            smoothingValue = self.smoothingSlider.field.value()
            xtalDMin = float(self.fieldXtalDMin.field.text())
            xtalDMax = float(self.fieldXtalDMax.field.text())
        except ValueError as e:
            QMessageBox.warning(
                self,
                "Invalid Peak Parameters",
                f"One of xtal dMin, xtal dMax, smoothing, or peak threshold is invalid: {str(e)}",
                QMessageBox.Ok,
            )
            return
        # perform some checks on dMin, dMax values
        if xtalDMin < 0.1:
            response = QMessageBox.warning(
                self,
                "Warning!!!",
                "Are you sure you want to do this? This may cause memory overflow or may take a long time to compute.",
                QMessageBox.Yes | QMessageBox.No,
            )
            if response == QMessageBox.No:
                return
        elif xtalDMin > xtalDMax:
            QMessageBox.warning(
                self,
                "Warning!!!",
                f"The minimum crystal d-spacing exceeds the maximum ({xtalDMax}). Please enter a smaller value.",
                QMessageBox.Ok,
            )
            return
        self.signalValueChanged.emit(index, smoothingValue, xtalDMin, xtalDMax)

    def updateWorkspaces(self, focusWorkspace, smoothedWorkspace, peaks):
        self.focusWorkspace = focusWorkspace
        self.smoothedWorkspace = smoothedWorkspace
        self.groupingSchema = self.groupingFileDropdown.currentText()
        self._updateGraphs(peaks)

    def _updateGraphs(self, peaks):
        # get the updated workspaces and optimal graph grid
        focusedWorkspace = mtd[self.focusWorkspace]
        smoothedWorkspace = mtd[self.smoothedWorkspace]
        peaks = pydantic.TypeAdapter(List[GroupPeakList]).validate_python(peaks)
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
            ax.axvline(x=max(min(x), float(self.fieldXtalDMin.field.text())), label="xtal $d_{min}$", color="red")
            ax.axvline(x=min(max(x), float(self.fieldXtalDMax.field.text())), label="xtal $d_{max}$", color="red")

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

    @Slot(bool)
    def setEnableRecalculateButton(self, enable):
        self.recalculationButton.setEnabled(enable)

    def disableRecalculateButton(self):
        self.signalUpdateRecalculationButton.emit(False)

    def enableRecalculateButton(self):
        self.signalUpdateRecalculationButton.emit(True)

    @Slot(list)
    def _populateGroupingDropdown(self, groups=["Enter a Run Number"]):
        self.groupingFileDropdown.setItems(groups)
        self.groupingFileDropdown.setEnabled(True)

    def populateGroupingDropdown(self, groups=["Enter a Run Number"]):
        self.signalPopulateGroupingDropdown.emit(groups)

    def verify(self):
        # TODO what needs to be verified?
        return True
