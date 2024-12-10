import matplotlib.pyplot as plt
from mantid.plots.datafunctions import get_spectrum
from mantid.simpleapi import mtd
from qtpy.QtCore import Signal, Slot
from qtpy.QtWidgets import (
    QFrame,
    QGridLayout,
    QHBoxLayout,
    QLineEdit,
    QMessageBox,
    QPushButton,
)
from workbench.plotting.figuremanager import MantidFigureCanvas
from workbench.plotting.toolbar import WorkbenchNavigationToolbar

from snapred.meta.Config import Config
from snapred.meta.decorators.Resettable import Resettable
from snapred.ui.view.BackendRequestView import BackendRequestView


@Resettable
class ArtificialNormalizationView(BackendRequestView):
    signalRunNumberUpdate = Signal(str)
    signalValueChanged = Signal(float, bool, bool, int)
    signalUpdateRecalculationButton = Signal(bool)
    signalUpdateFields = Signal(float, bool, bool)

    def __init__(self, parent=None):
        super().__init__(parent=parent)

        # create the run number field
        self.fieldRunNumber = self._labeledField("Run Number", QLineEdit())
        self.fieldRunNumber.setEnabled(False)

        # create the graph elements
        self.figure = plt.figure(constrained_layout=True)
        self.canvas = MantidFigureCanvas(self.figure)
        self.navigationBar = WorkbenchNavigationToolbar(self.canvas, self)

        # create the other specification elements
        self.lssDropdown = self._trueFalseDropDown("LSS")
        self.decreaseParameterDropdown = self._trueFalseDropDown("Decrease Parameter")

        # create the adjustment controls
        self.smoothingSlider = self._labeledField("Smoothing", QLineEdit())
        self.smoothingSlider.field.setText(str(Config["ui.default.reduction.smoothing"]))

        self.peakWindowClippingSize = self._labeledField(
            "Peak Window Clipping Size",
            QLineEdit(str(Config["constants.ArtificialNormalization.peakWindowClippingSize"])),
        )

        peakControlLayout = QHBoxLayout()
        peakControlLayout.addWidget(self.smoothingSlider, 2)
        peakControlLayout.addWidget(self.peakWindowClippingSize)

        # a big ol recalculate button
        self.recalculationButton = QPushButton("Recalculate")
        self.recalculationButton.clicked.connect(self.emitValueChange)

        # add all elements to an adjust layout
        self.adjustLayout = QGridLayout()
        self.adjustLayout.addWidget(self.fieldRunNumber, 0, 0)
        self.adjustLayout.addWidget(self.navigationBar, 1, 0)
        self.adjustLayout.addWidget(self.canvas, 2, 0, 1, -1)
        self.adjustLayout.addLayout(peakControlLayout, 3, 0, 1, 2)
        self.adjustLayout.addWidget(self.lssDropdown, 4, 0)
        self.adjustLayout.addWidget(self.decreaseParameterDropdown, 4, 1)
        self.adjustLayout.addWidget(self.recalculationButton, 5, 0, 1, 2)

        self.adjustLayout.setRowStretch(2, 10)

        # add the adjust layout to this layout so it may be turned on and off
        self.adjustFrame = QFrame()
        self.adjustFrame.setLayout(self.adjustLayout)
        self.layout.addWidget(self.adjustFrame, 0, 0, -1, -1)
        self.adjustFrame.show()

        # store the initial layout height without graphs
        self.initialLayoutHeight = self.size().height()

        self.signalUpdateRecalculationButton.connect(self.setEnableRecalculateButton)
        self.signalUpdateFields.connect(self._updateFields)
        self.signalRunNumberUpdate.connect(self._updateRunNumber)

    @Slot(str)
    def _updateRunNumber(self, runNumber):
        self.fieldRunNumber.setText(runNumber)

    def updateRunNumber(self, runNumber):
        self.signalRunNumberUpdate.emit(runNumber)

    @Slot(float, bool, bool)
    def _updateFields(self, smoothingParameter, lss, decreaseParameter):
        self.smoothingSlider.field.setValue(smoothingParameter)
        self.lssDropdown.setCurrentIndex(lss)
        self.decreaseParameterDropdown.setCurrentIndex(decreaseParameter)

    def updateFields(self, smoothingParameter, lss, decreaseParameter):
        self.signalUpdateFields.emit(smoothingParameter, lss, decreaseParameter)

    @Slot()
    def emitValueChange(self):
        # verify the fields before recalculation
        try:
            smoothingValue = float(self.smoothingSlider.field.text())
            lss = self.lssDropdown.getValue()
            decreaseParameter = self.decreaseParameterDropdown.getValue()
            peakWindowClippingSize = int(self.peakWindowClippingSize.field.text())
        except ValueError as e:
            QMessageBox.warning(
                self,
                "Invalid Peak Parameters",
                f"Smoothing or peak window clipping size is invalid: {str(e)}",
                QMessageBox.Ok,
            )
            return
        self.signalValueChanged.emit(smoothingValue, lss, decreaseParameter, peakWindowClippingSize)

    def updateWorkspaces(self, diffractionWorkspace, artificialNormWorkspace):
        self.diffractionWorkspace = diffractionWorkspace
        self.artificialNormWorkspace = artificialNormWorkspace
        self._updateGraphs()

    def _updateGraphs(self):
        # get the updated workspaces and optimal graph grid
        diffractionWorkspace = mtd[self.diffractionWorkspace]
        artificialNormWorkspace = mtd[self.artificialNormWorkspace]
        numGraphs = diffractionWorkspace.getNumberHistograms()
        nrows, ncols = self._optimizeRowsAndCols(numGraphs)

        # now re-draw the figure
        self.figure.clear()
        for i in range(numGraphs):
            ax = self.figure.add_subplot(nrows, ncols, i + 1, projection="mantid")
            ax.plot(diffractionWorkspace, wkspIndex=i, label="Diffcal Data", normalize_by_bin_width=True)
            ax.plot(
                artificialNormWorkspace,
                wkspIndex=i,
                label="Artificial Normalization Data",
                normalize_by_bin_width=True,
                linestyle="--",
            )
            ax.legend()
            ax.tick_params(direction="in")
            ax.set_title(f"Group ID: {i + 1}")
            # fill in the discovered peaks for easier viewing
            x, y, _, _ = get_spectrum(diffractionWorkspace, i, normalize_by_bin_width=True)
            # for each detected peak in this group, shade in the peak region

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

    def verify(self):
        # TODO what needs to be verified?
        return True

    def showSkippedView(self):
        self.adjustFrame.hide()

    def showAdjustView(self):
        self.adjustFrame.show()

    def getPeakWindowClippingSize(self):
        return int(self.peakWindowClippingSize.field.text())

    def getSmoothingParameter(self):
        return float(self.smoothingSlider.field.text())

    def getLSS(self):
        return self.lssDropdown.getValue()

    def getDecreaseParameter(self):
        return self.decreaseParameterDropdown.getValue()
