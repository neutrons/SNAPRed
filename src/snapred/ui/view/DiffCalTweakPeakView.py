import math
import unittest.mock as mock
from typing import List

import matplotlib.pyplot as plt
import numpy as np
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
from workbench.plotting.figuremanager import FigureManagerWorkbench, MantidFigureCanvas
from workbench.plotting.toolbar import WorkbenchNavigationToolbar

from snapred.backend.dao import GroupPeakList
from snapred.backend.dao.Limit import Pair
from snapred.backend.error.ContinueWarning import ContinueWarning
from snapred.backend.recipe.algorithm.FitMultiplePeaksAlgorithm import FitOutputEnum
from snapred.meta.Config import Config
from snapred.meta.decorators.Resettable import Resettable
from snapred.meta.mantid.AllowedPeakTypes import SymmetricPeakEnum
from snapred.ui.view.BackendRequestView import BackendRequestView
from snapred.ui.widget.Toggle import Toggle


@Resettable
class DiffCalTweakPeakView(BackendRequestView):
    """

    The DiffCalTweakPeakView is a view within SNAPRed, aimed at adjusting and visualizing diffraction calibration peaks.
    This view integrates a mix of user input fields, graphical display elements, and interactive controls for precise
    calibration tweaking. It provides a structured layout for inputting calibration parameters, alongside a dynamic
    matplotlib graph for real-time visualization of calibration peaks against specified thresholds and ranges.

    """

    DMIN = Config["constants.CrystallographicInfo.dMin"]
    DMAX = Config["constants.CrystallographicInfo.dMax"]
    THRESHOLD = Config["constants.PeakIntensityFractionThreshold"]
    MIN_PEAKS = Config["calibration.diffraction.minimumPeaksPerGroup"]
    PREF_PEAKS = Config["calibration.diffraction.preferredPeaksPerGroup"]
    MAX_CHI_SQ = Config["constants.GroupDiffractionCalibration.MaxChiSq"]
    FWHM = Pair.parse_obj(Config["calibration.parameters.default.FWHMMultiplier"])

    signalRunNumberUpdate = Signal(str)
    signalPeakThresholdUpdate = Signal(float)
    signalValueChanged = Signal(int, float, float, float, SymmetricPeakEnum, Pair)
    signalUpdateRecalculationButton = Signal(bool)

    def __init__(self, jsonForm, samples=[], groups=[], parent=None):
        selection = "calibration/diffractionCalibration"
        super().__init__(jsonForm, selection, parent=parent)

        # create the run number field and lite mode toggle
        self.runNumberField = self._labeledField("Run Number")
        self.litemodeToggle = self._labeledField("Lite Mode", Toggle(parent=self, state=True))
        self.signalRunNumberUpdate.connect(self._updateRunNumber)
        self.signalPeakThresholdUpdate.connect(self._updatePeakThreshold)

        # create the graph elements
        self.figure = plt.figure(constrained_layout=True)
        self.canvas = MantidFigureCanvas(self.figure)
        self.navigationBar = WorkbenchNavigationToolbar(self.canvas, self)

        # create the dropdowns
        self.sampleDropdown = self._sampleDropDown("Sample", samples)
        self.groupingFileDropdown = self._sampleDropDown("Grouping File", groups)
        self.peakFunctionDropdown = self._sampleDropDown("Peak Function", [p.value for p in SymmetricPeakEnum])

        # disable run number, lite mode, sample, peak fucnction -- cannot be changed now
        for x in [self.runNumberField, self.litemodeToggle, self.sampleDropdown]:
            x.setEnabled(False)

        # create the peak adustment controls
        self.fielddMin = self._labeledField("dMin", QLineEdit(str(self.DMIN)))
        self.fielddMax = self._labeledField("dMax", QLineEdit(str(self.DMAX)))
        self.fieldFWHMleft = self._labeledField("FWHM left", QLineEdit(str(self.FWHM.left)))
        self.fieldFWHMright = self._labeledField("FWHM right", QLineEdit(str(self.FWHM.right)))
        self.fieldThreshold = self._labeledField("intensity threshold", QLineEdit(str(self.THRESHOLD)))
        peakControlLayout = QHBoxLayout()
        peakControlLayout.addWidget(self.fielddMin)
        peakControlLayout.addWidget(self.fielddMax)
        peakControlLayout.addWidget(self.fieldFWHMleft)
        peakControlLayout.addWidget(self.fieldFWHMright)
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

    def _updatePeakThreshold(self, peakThreshold):
        self.fieldThreshold.setText(str(peakThreshold))

    def updatePeakThreshold(self, peakThreshold):
        self.signalPeakThresholdUpdate.emit(float(peakThreshold))

    def updateFields(self, sampleIndex, groupingIndex, peakIndex):
        self.sampleDropdown.setCurrentIndex(sampleIndex)
        self.groupingFileDropdown.setCurrentIndex(groupingIndex)
        self.peakFunctionDropdown.setCurrentIndex(peakIndex)

    def emitValueChange(self):
        # verify the fields before recalculation
        try:
            groupingIndex = self.groupingFileDropdown.currentIndex()
            dMin = float(self.fielddMin.field.text())
            dMax = float(self.fielddMax.field.text())
            peakThreshold = float(self.fieldThreshold.text())
            peakFunction = SymmetricPeakEnum(self.peakFunctionDropdown.currentText())
            fwhm = Pair(
                left=float(self.fieldFWHMleft.text()),
                right=float(self.fieldFWHMright.text()),
            )
        except ValueError as e:
            QMessageBox.warning(
                self,
                "Invalid Peak Parameters",
                f"One of dMin, dMax, or peak threshold is invalid: {str(e)}",
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
        self.signalValueChanged.emit(groupingIndex, dMin, dMax, peakThreshold, peakFunction, fwhm)

    def updateGraphs(self, workspace, peaks, diagnostic):
        # get the updated workspaces and optimal graph grid
        self.peaks = parse_obj_as(List[GroupPeakList], peaks)
        numGraphs = len(peaks)
        self.goodPeaksCount = [0] * numGraphs
        nrows, ncols = self._optimizeRowsAndCols(numGraphs)

        # now re-draw the figure
        self.figure.clear()
        incr = len(FitOutputEnum)
        for wkspIndex in range(numGraphs):
            peaks = self.peaks[wkspIndex].peaks
            # collect the fit chi-sq parameters for this spectrum, and the fits
            fitted_peaks = mtd[diagnostic].getItem(incr * wkspIndex + FitOutputEnum.Workspace.value)
            param_table = mtd[diagnostic].getItem(incr * wkspIndex + FitOutputEnum.Parameters.value).toDict()
            chisq = param_table["chi2"]
            self.goodPeaksCount[wkspIndex] = len([peak for chi2, peak in zip(chisq, peaks) if chi2 < self.MAX_CHI_SQ])
            # prepare the plot area
            ax = self.figure.add_subplot(nrows, ncols, wkspIndex + 1, projection="mantid")
            ax.tick_params(direction="in")
            ax.set_title(f"Group ID: {wkspIndex + 1}")
            # plot the data
            ax.plot(mtd[workspace], wkspIndex=wkspIndex, label="data", normalize_by_bin_width=True)
            # plot the fitted peaks
            ax.plot(fitted_peaks, wkspIndex=0, label="fit", color="black", normalize_by_bin_width=True)
            ax.legend(loc=1)
            # fill in the discovered peaks for easier viewing
            x, y, _, _ = get_spectrum(mtd[workspace], wkspIndex, normalize_by_bin_width=True)
            # for each detected peak in this group, shade in the peak region
            for chi2, peak in zip(chisq, peaks):
                # areas inside peak bounds (to be shaded)
                under_peaks = [(peak.minimum < xx and xx < peak.maximum) for xx in x]
                # the color: blue = GOOD, red = BAD
                color = "blue" if chi2 < self.MAX_CHI_SQ else "red"
                alpha = 0.3 if chi2 < self.MAX_CHI_SQ else 0.8
                # now shade
                ax.fill_between(x, y, where=under_peaks, color=color, alpha=alpha)
            # plot the min and max value for peaks
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

    def _testFailStates(self):
        empties = [gpl for gpl, count in zip(self.peaks, self.goodPeaksCount) if count < self.MIN_PEAKS]
        if len(empties) > 0:
            msg = f"Proper calibration requires at least {self.MIN_PEAKS} well-fit peaks per group.\n"
            for empty in empties:
                msg = msg + f"\tgroup {empty.groupID} has \t {len(empty.peaks)} peaks\n"
            msg = msg + "Adjust grouping, dMin, dMax, and peak intensity threshold to include more peaks."
            raise ValueError(msg)
        badPeaks = [gpl for gpl, count in zip(self.peaks, self.goodPeaksCount) if len(gpl.peaks) != count]
        if len(badPeaks) > 0:
            msg = "Peaks in the following groups have chi-squared values exceeding the maximum allowed value.\n"
            for badPeak in badPeaks:
                msg = msg + f"\tgroup {badPeak.groupID} has bad peaks at \t {[peak.value for peak in badPeak.peaks]}\n"
            msg = msg + "Adjust FWHM, dMin, dMax, peak intensity threshold, ect. to better fit more peaks."
            raise ValueError(msg)

    def _testContinueAnywayStates(self):
        tooFews = [gpl for gpl, count in zip(self.peaks, self.goodPeaksCount) if count < self.PREF_PEAKS]
        if len(tooFews) > 0:
            msg = f"It is recommended to have at least {self.PREF_PEAKS} well-fit peaks per group.\n"
            for tooFew in tooFews:
                msg = msg + f"\tgroup {tooFew.groupID} has \t {len(tooFew.peaks)} peaks\n"
            msg = msg + "Would you like to continue anyway?"
            raise ContinueWarning(msg)

    def verify(self):
        self._testFailStates()
        self._testContinueAnywayStates()

        return True
