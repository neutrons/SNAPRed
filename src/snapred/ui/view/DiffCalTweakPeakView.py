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
from snapred.backend.dao.Limit import Pair
from snapred.backend.error.ContinueWarning import ContinueWarning
from snapred.meta.Config import Config
from snapred.meta.decorators.Resettable import Resettable
from snapred.meta.mantid.AllowedPeakTypes import SymmetricPeakEnum
from snapred.meta.mantid.FitPeaksOutput import FitOutputEnum
from snapred.ui.view.BackendRequestView import BackendRequestView


@Resettable
class DiffCalTweakPeakView(BackendRequestView):
    """

    The DiffCalTweakPeakView is a view within SNAPRed, aimed at adjusting and visualizing diffraction calibration peaks.
    This view integrates a mix of user input fields, graphical display elements, and interactive controls for precise
    calibration tweaking. It provides a structured layout for inputting calibration parameters, alongside a dynamic
    matplotlib graph for real-time visualization of calibration peaks against specified thresholds and ranges.

    """

    XTAL_DMIN = Config["constants.CrystallographicInfo.crystalDMin"]
    XTAL_DMAX = Config["constants.CrystallographicInfo.crystalDMax"]
    MIN_PEAKS = Config["calibration.diffraction.minimumPeaksPerGroup"]
    PREF_PEAKS = Config["calibration.diffraction.preferredPeaksPerGroup"]
    MAX_CHI_SQ = Config["constants.GroupDiffractionCalibration.MaxChiSq"]
    FWHM = Pair.model_validate(Config["calibration.parameters.default.FWHMMultiplier"])

    FIGURE_MARGIN = 0.5  # top + bottom: inches

    signalRunNumberUpdate = Signal(str)
    signalValueChanged = Signal(int, float, float, SymmetricPeakEnum, Pair, float)
    signalUpdateRecalculationButton = Signal(bool)
    signalMaxChiSqUpdate = Signal(float)
    signalContinueAnyway = Signal(bool)
    signalPurgeBadPeaks = Signal(float)

    def __init__(self, samples=[], groups=[], parent=None):
        super().__init__(parent=parent)

        # create the run number field and lite mode toggle
        self.runNumberField = self._labeledField("Run Number")
        self.litemodeToggle = self._labeledToggle("Lite Mode", True)
        self.maxChiSqField = self._labeledField("Max Chi Sq", text=str(self.MAX_CHI_SQ))
        self.signalRunNumberUpdate.connect(self._updateRunNumber)
        self.signalMaxChiSqUpdate.connect(self._updateMaxChiSq)

        self.continueAnyway = False
        self.signalContinueAnyway.connect(self._updateContinueAnyway)

        # create the graph elements
        self.figure = plt.figure(constrained_layout=True)
        self.canvas = MantidFigureCanvas(self.figure)
        self.navigationBar = WorkbenchNavigationToolbar(self.canvas, self)

        # create the dropdowns
        self.sampleDropdown = self._sampleDropDown("Sample", samples)
        self.groupingFileDropdown = self._sampleDropDown("Grouping File", groups)
        self.peakFunctionDropdown = self._sampleDropDown("Peak Function", [p.value for p in SymmetricPeakEnum])

        # disable run number, lite mode, sample, peak function -- cannot be changed now
        for x in [self.runNumberField, self.litemodeToggle, self.sampleDropdown]:
            x.setEnabled(False)

        # create the peak adjustment controls
        self.fieldXtalDMin = self._labeledField("xtal dMin", text=str(self.XTAL_DMIN))
        self.fieldXtalDMax = self._labeledField("xtal dMax", text=str(self.XTAL_DMAX))
        self.fieldFWHMleft = self._labeledField("FWHM left", text=str(self.FWHM.left))
        self.fieldFWHMright = self._labeledField("FWHM right", text=str(self.FWHM.right))
        peakControlLayout = QHBoxLayout()
        peakControlLayout.addWidget(self.fieldXtalDMin)
        peakControlLayout.addWidget(self.fieldXtalDMax)
        peakControlLayout.addWidget(self.fieldFWHMleft)
        peakControlLayout.addWidget(self.fieldFWHMright)
        peakControlLayout.addWidget(self.maxChiSqField)

        # a big ol recalculate button
        self.recalculationButton = QPushButton("Recalculate")
        self.recalculationButton.clicked.connect(self.emitValueChange)

        # a little ol purge bad peaks button
        self.purgePeaksButton = QPushButton("Purge Bad Peaks")
        self.purgePeaksButton.clicked.connect(self.emitPurge)

        # add all elements to the grid layout
        self.layout.addWidget(self.runNumberField, 0, 0)
        self.layout.addWidget(self.litemodeToggle, 0, 1)
        self.layout.addWidget(self.navigationBar, 1, 0)
        self.layout.addWidget(self.canvas, 2, 0, 1, -1)
        self.layout.addLayout(peakControlLayout, 3, 0, 1, 2)
        self.layout.addWidget(self.sampleDropdown, 4, 0)
        self.layout.addWidget(self.groupingFileDropdown, 4, 1)
        self.layout.addWidget(self.peakFunctionDropdown, 4, 2)
        self.layout.addWidget(self.purgePeaksButton, 4, 3)
        self.layout.addWidget(self.recalculationButton, 5, 0, 1, 4)

        self.layout.setRowStretch(2, 10)

        # store the initial layout without graphs
        self.initialLayoutHeight = self.size().height()

        self.signalUpdateRecalculationButton.connect(self.setEnableRecalculateButton)

    def updateContinueAnyway(self, continueAnyway: bool):
        self.signalContinueAnyway.emit(continueAnyway)

    @Slot(bool)
    def _updateContinueAnyway(self, continueAnyway: bool):
        self.continueAnyway = continueAnyway

    @Slot(str)
    def _updateRunNumber(self, runNumber):
        self.runNumberField.setText(runNumber)

    def updateRunNumber(self, runNumber):
        self.signalRunNumberUpdate.emit(runNumber)

    @Slot(float)
    def _updateMaxChiSq(self, maxChiSq: float):
        self.maxChiSqField.setText(str(maxChiSq))

    def updateMaxChiSq(self, maxChiSq):
        self.signalMaxChiSqUpdate.emit(maxChiSq)

    def updateFields(self, sampleIndex, groupingIndex, peakIndex):
        self.sampleDropdown.setCurrentIndex(sampleIndex)
        self.groupingFileDropdown.setCurrentIndex(groupingIndex)
        self.peakFunctionDropdown.setCurrentIndex(peakIndex)

    @Slot()
    def emitValueChange(self):
        # verify the fields before recalculation
        try:
            groupingIndex = self.groupingFileDropdown.currentIndex()
            xtalDMin = float(self.fieldXtalDMin.field.text())
            xtalDMax = float(self.fieldXtalDMax.field.text())
            peakFunction = SymmetricPeakEnum(self.peakFunctionDropdown.currentText())
            fwhm = Pair(
                left=float(self.fieldFWHMleft.text()),
                right=float(self.fieldFWHMright.text()),
            )
            maxChiSq = float(self.maxChiSqField.text())
        except ValueError as e:
            QMessageBox.warning(
                self,
                "Invalid Peak Parameters",
                f"One of xtal dMin, xtal dMax, peak threshold, or max chi sq is invalid: {str(e)}",
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
                f"The minimum crystal d-spacing exceeds the maximum ({xtalDMax}). Please adjust values.",
                QMessageBox.Ok,
            )
            return
        self.signalValueChanged.emit(groupingIndex, xtalDMin, xtalDMax, peakFunction, fwhm, maxChiSq)

    @Slot()
    def emitPurge(self):
        try:
            maxChiSq = float(self.maxChiSqField.text())
        except ValueError:
            QMessageBox.warning(
                self,
                "Invalid Chi-Squared Maximum",
                "Enter a valid value for the maximum chi-squared before purging.",
                QMessageBox.Ok,
            )
            return
        self.signalPurgeBadPeaks.emit(maxChiSq)

    def updateGraphs(self, workspace, peaks, diagnostic, residual):
        # get the updated workspaces and optimal graph grid
        self.peaks = pydantic.TypeAdapter(List[GroupPeakList]).validate_python(peaks)
        numGraphs = len(peaks)
        self.goodPeaksCount = [0] * numGraphs
        self.badPeaks = [[]] * numGraphs
        nrows, ncols = self._optimizeRowsAndCols(numGraphs)
        fitted_peaks = mtd[diagnostic].getItem(FitOutputEnum.Workspace.value)
        param_table = mtd[diagnostic].getItem(FitOutputEnum.Parameters.value).toDict()
        index = param_table["wsindex"]
        allChisq = param_table["chi2"]
        maxChiSq = float(self.maxChiSqField.text())

        # now re-draw the figure
        self.figure.clear()
        for wkspIndex in range(numGraphs):
            peaks = self.peaks[wkspIndex].peaks
            # collect the fit chi-sq parameters for this spectrum, and the fits
            chisq = [x2 for i, x2 in zip(index, allChisq) if i == wkspIndex]
            self.goodPeaksCount[wkspIndex] = len([peak for chi2, peak in zip(chisq, peaks) if chi2 < maxChiSq])
            self.badPeaks[wkspIndex] = [peak for chi2, peak in zip(chisq, peaks) if chi2 >= maxChiSq]
            # prepare the plot area
            ax = self.figure.add_subplot(nrows, ncols, wkspIndex + 1, projection="mantid")
            ax.tick_params(direction="in")
            ax.set_title(f"Group ID: {wkspIndex + 1}")
            # plot the data and fitted curve
            ax.plot(mtd[workspace], wkspIndex=wkspIndex, label="data", normalize_by_bin_width=True)
            ax.plot(fitted_peaks, wkspIndex=wkspIndex, label="fit", color="black", normalize_by_bin_width=True)

            # plot the residual data
            ax.plot(
                mtd[residual],
                wkspIndex=wkspIndex,
                label="residual",
                color="limegreen",
                linewidth=2,
                normalize_by_bin_width=True,
            )

            ax.legend(loc=1)

            # fill in the discovered peaks for easier viewing
            x, y, _, _ = get_spectrum(mtd[workspace], wkspIndex, normalize_by_bin_width=True)
            # for each detected peak in this group, shade in the peak region
            for chi2, peak in zip(chisq, peaks):
                # areas inside peak bounds (to be shaded)
                under_peaks = [(peak.minimum < xx and xx < peak.maximum) for xx in x]
                # the color: blue = GOOD, red = BAD
                color = "blue" if chi2 < maxChiSq else "red"
                alpha = 0.3 if chi2 < maxChiSq else 0.8
                # now shade
                ax.fill_between(x, y, where=under_peaks, color=color, alpha=alpha)
            # plot the min and max value for peaks
            ax.axvline(x=max(min(x), float(self.fieldXtalDMin.field.text())), label="xtal $d_{min}$", color="red")
            ax.axvline(x=min(max(x), float(self.fieldXtalDMax.field.text())), label="xtal $d_{max}$", color="red")

        # resize window and redraw
        self.setMinimumHeight(
            self.initialLayoutHeight + int((self.figure.get_size_inches()[1] + self.FIGURE_MARGIN) * self.figure.dpi)
        )
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

    def populateGroupingDropdown(self, groups=["Enter a Run Number"]):
        self.groupingFileDropdown.setItems(groups)
        self.groupingFileDropdown.setEnabled(True)

    def _testFailStates(self):
        empties = [gpl for gpl, count in zip(self.peaks, self.goodPeaksCount) if count < self.MIN_PEAKS]
        if len(empties) > 0:
            msg = f"Proper calibration requires at least {self.MIN_PEAKS} well-fit peaks per group.\n"
            for empty in empties:
                msg = msg + f"\tgroup {empty.groupID} has \t {len(empty.peaks)} peaks\n"
            msg = msg + "Adjust grouping, xtal dMin, xtal dMax, and peak intensity threshold to include more peaks."
            raise ValueError(msg)
        totalBadPeaks = sum([len(badPeaks) for badPeaks in self.badPeaks])
        if totalBadPeaks > 0:
            msg = "Peaks in the following groups have chi-squared values exceeding the maximum allowed value.\n"
            for groupID, badPeak in enumerate(self.badPeaks):
                msg = msg + f"\tgroup {groupID + 1} has bad peaks at {[peak.value for peak in badPeak]}\n"
            msg = msg + "Adjust FWHM, xtal dMin, xtal dMax, peak intensity threshold, etc. to better fit more peaks."
            raise ValueError(msg)

    def _testContinueAnywayStates(self):
        tooFews = [gpl for gpl, count in zip(self.peaks, self.goodPeaksCount) if count < self.PREF_PEAKS]
        if len(tooFews) > 0:
            msg = f"It is recommended to have at least {self.PREF_PEAKS} well-fit peaks per group.\n"
            for tooFew in tooFews:
                msg = msg + f"\tgroup {tooFew.groupID} has \t {len(tooFew.peaks)} peaks\n"
            msg = msg + "Would you like to continue anyway?"
            raise ContinueWarning(msg, ContinueWarning.Type.LOW_PEAK_COUNT)

    def verify(self):
        self._testFailStates()
        if not self.continueAnyway:
            self._testContinueAnywayStates()

        return True
