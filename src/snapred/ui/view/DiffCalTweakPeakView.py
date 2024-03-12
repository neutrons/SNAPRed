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
    QLineEdit,
    QMessageBox,
    QPushButton,
    QWidget,
)
from workbench.plotting.figuremanager import FigureManagerWorkbench, MantidFigureCanvas
from workbench.plotting.toolbar import WorkbenchNavigationToolbar

from snapred.backend.dao import GroupPeakList
from snapred.meta.Config import Config
from snapred.meta.decorators.Resettable import Resettable
from snapred.meta.mantid.AllowedPeakTypes import SymmetricPeakEnum
from snapred.ui.view.BackendRequestView import BackendRequestView
from snapred.ui.widget.JsonFormList import JsonFormList
from snapred.ui.widget.LabeledField import LabeledField
from snapred.ui.widget.Toggle import Toggle


@Resettable
class DiffCalTweakPeakView(BackendRequestView):
    signalRunNumberUpdate = pyqtSignal(str)
    """
    """
    signalValueChanged = pyqtSignal(int, float, float, float)
    """
    """
    signalUpdateRecalculationButton = pyqtSignal(bool)
    """
    """

    DMIN = Config["constants.CrystallographicInfo.dMin"]
    DMAX = Config["constants.CrystallographicInfo.dMax"]
    THRESHOLD = Config["constants.PeakIntensityFractionThreshold"]

    def __init__(self, jsonForm, samples=[], groups=[], parent=None):
        selection = "calibration/diffractionCalibration"
        super().__init__(jsonForm, selection, parent=parent)

        # create the run number field and lite mode toggle
        self.runNumberField = self._labeledField("Run Number")
        self.litemodeToggle = self._labeledField("Lite Mode", Toggle(parent=self, state=True))
        self.signalRunNumberUpdate.connect(self._updateRunNumber)

        # create the graph elements
        self.figure = plt.figure(constrained_layout=True)
        self.canvas = MantidFigureCanvas(self.figure)
        self.navigationBar = WorkbenchNavigationToolbar(self.canvas, self)

        # create the dropdowns
        self.sampleDropdown = self._sampleDropDown("Sample", samples)
        self.groupingFileDropdown = self._sampleDropDown("Grouping File", groups)
        self.peakFunctionDropdown = self._sampleDropDown("Peak Function", [p.value for p in SymmetricPeakEnum])

        # disable run number, lite mode, sample, peak fucnction -- cannot be changed now
        for x in [self.runNumberField, self.litemodeToggle, self.sampleDropdown, self.peakFunctionDropdown]:
            x.setEnabled(False)

        # create the peak adustment controls
        self.fielddMin = self._labeledField("dMin", QLineEdit(str(self.DMIN)))
        self.fielddMax = self._labeledField("dMax", QLineEdit(str(self.DMAX)))
        self.fieldThreshold = self._labeledField("intensity threshold", QLineEdit(str(self.THRESHOLD)))
        peakControlLayout = QHBoxLayout()
        peakControlLayout.addWidget(self.fielddMin)
        peakControlLayout.addWidget(self.fielddMax)
        peakControlLayout.addWidget(self.fieldThreshold)

        # a big ol recalculate button
        self.recalculationButton = QPushButton("Recalculate")
        self.recalculationButton.clicked.connect(self.emitValueChange)

        # add all elements to the grid layout
        self.layout.addWidget(self.runNumberField, 0, 0)
        self.layout.addWidget(self.litemodeToggle, 0, 1)
        self.layout.addWidget(self.navigationBar, 1, 0)
        self.layout.addWidget(self.canvas, 2, 0, 1, -1)
        self.layout.addLayout(peakControlLayout, 3, 0, 1, 2)
        self.layout.addWidget(self.sampleDropdown, 4, 0)
        self.layout.addWidget(self.groupingFileDropdown, 4, 1)
        self.layout.addWidget(self.peakFunctionDropdown, 4, 2)
        self.layout.addWidget(self.recalculationButton, 5, 0, 1, 2)

        self.layout.setRowStretch(2, 10)

        # store the initial layout without graphs
        self.initialLayoutHeight = self.size().height()

        self.signalUpdateRecalculationButton.connect(self.setEnableRecalculateButton)

    def _updateRunNumber(self, runNumber):
        self.runNumberField.setText(runNumber)

    def updateRunNumber(self, runNumber):
        self.signalRunNumberUpdate.emit(runNumber)

    def updateFields(self, runNumber, sampleIndex, groupingIndex, peakIndex):  # noqa ARG002
        # NOTE uncommenting the below -- inexplicably -- causes a segfault
        # self.runNumberField.setText(runNumber)
        self.sampleDropdown.setCurrentIndex(sampleIndex)
        self.groupingFileDropdown.setCurrentIndex(groupingIndex)
        self.peakFunctionDropdown.setCurrentIndex(peakIndex)

    def emitValueChange(self):
        groupingIndex = self.groupingFileDropdown.currentIndex()
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
        self.signalValueChanged.emit(groupingIndex, dMin, dMax, peakThreshold)

    def updateGraphs(self, workspace, peaks):
        # get the updated workspaces and optimal graph grid
        self.peaks = parse_obj_as(List[GroupPeakList], peaks)
        numGraphs = len(peaks)
        nrows, ncols = self._optimizeRowsAndCols(numGraphs)

        # now re-draw the figure
        self.figure.clear()
        for i in range(numGraphs):
            ax = self.figure.add_subplot(nrows, ncols, i + 1, projection="mantid")
            ax.plot(mtd[workspace], wkspIndex=i, label="data", normalize_by_bin_width=True)
            ax.legend()
            ax.tick_params(direction="in")
            ax.set_title(f"Group ID: {i + 1}")
            # fill in the discovered peaks for easier viewing
            x, y, _, _ = get_spectrum(mtd[workspace], i, normalize_by_bin_width=True)
            # for each detected peak in this group, shade in the peak region
            for peak in self.peaks[i].peaks:
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

    def populateGroupingDropdown(self, groups=["Enter a Run Number"]):
        self.groupingFileDropdown.setItems(groups)
        self.groupingFileDropdown.setEnabled(True)

    def verify(self):
        empties = [gpl for gpl in self.peaks if len(gpl.peaks) < 4]
        if len(empties) > 0:
            raise ValueError(
                "Proper calibration requires at least 4 peaks per group.  "
                + f"Groups {[empty.groupID for empty in empties]} have "
                + f"{[len(empty.peaks) for empty in empties]} peaks.  "
                + "Adjust grouping, dMin, dMax, and peak intensity threshold to include more peaks."
            )
        return True
