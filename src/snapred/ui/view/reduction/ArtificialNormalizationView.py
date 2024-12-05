import matplotlib.pyplot as plt
from mantid.plots.datafunctions import get_spectrum
from mantid.simpleapi import mtd
from qtpy.QtCore import Qt, Signal, Slot
from qtpy.QtWidgets import (
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPushButton,
)
from snapred.meta.Config import Config
from snapred.meta.decorators.Resettable import Resettable
from snapred.ui.view.BackendRequestView import BackendRequestView
from workbench.plotting.figuremanager import MantidFigureCanvas
from workbench.plotting.toolbar import WorkbenchNavigationToolbar


@Resettable
class ArtificialNormalizationView(BackendRequestView):
    signalRunNumberUpdate = Signal(str)
    signalValueChanged = Signal(float, bool, bool, int)
    signalUpdateRecalculationButton = Signal(bool)
    signalUpdateFields = Signal(float, bool, bool)

    def __init__(self, parent=None):
        super().__init__(parent=parent)

        # create the run number fields
        self.fieldRunNumber = self._labeledField("Run Number", QLineEdit())

        # create the graph elements
        self.figure = plt.figure(constrained_layout=True)
        self.canvas = MantidFigureCanvas(self.figure)
        self.navigationBar = WorkbenchNavigationToolbar(self.canvas, self)

        # create the other specification elements
        self.lssDropdown = self._trueFalseDropDown("LSS")
        self.decreaseParameterDropdown = self._trueFalseDropDown("Decrease Parameter")

        # disable run number
        for x in [self.fieldRunNumber]:
            x.setEnabled(False)

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

        # add all elements to the grid layout
        self.layout.addWidget(self.fieldRunNumber, 0, 0)
        self.layout.addWidget(self.navigationBar, 1, 0)
        self.layout.addWidget(self.canvas, 2, 0, 1, -1)
        self.layout.addLayout(peakControlLayout, 3, 0, 1, 2)
        self.layout.addWidget(self.lssDropdown, 4, 0)
        self.layout.addWidget(self.decreaseParameterDropdown, 4, 1)
        self.layout.addWidget(self.recalculationButton, 5, 0, 1, 2)

        self.layout.setRowStretch(2, 10)

        # store the initial layout without graphs
        self.initialLayoutHeight = self.size().height()

        self.signalUpdateRecalculationButton.connect(self.setEnableRecalculateButton)
        self.signalUpdateFields.connect(self._updateFields)
        self.signalRunNumberUpdate.connect(self._updateRunNumber)

        self.messageLabel = QLabel("")
        self.messageLabel.setStyleSheet("font-size: 24px; font-weight: bold; color: black;")
        self.messageLabel.setAlignment(Qt.AlignCenter)
        self.layout.addWidget(self.messageLabel, 0, 0, 1, 2)
        self.messageLabel.hide()

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
            lss = self.lssDropdown.currentIndex() == "True"
            decreaseParameter = self.decreaseParameterDropdown.currentIndex == "True"
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

            # NOTE: Mutate the ax object as the mantidaxis does not account for lines
            # TODO: Bubble this up to the mantid codebase and remove this mutation.
            def rename_workspace(old_name, new_name):
                """
                Rename a workspace, and update the artists, creation arguments and tracked workspaces accordingly
                :param new_name : the new name of workspace
                :param old_name : the old name of workspace
                """
                for cargs in ax.creation_args:
                    # NEW CHECK
                    func_name = cargs["function"]
                    if func_name not in ["axhline", "axvline"] and cargs["workspaces"] == old_name:
                        cargs["workspaces"] = new_name
                    # Alternatively,
                    # if cargs.get("workspaces") == old_name:
                    #     cargs["workspaces"] = new_name
                for ws_name, ws_artist_list in list(ax.tracked_workspaces.items()):
                    for ws_artist in ws_artist_list:
                        if ws_artist.workspace_name == old_name:
                            ws_artist.rename_data(new_name)
                    if ws_name == old_name:
                        ax.tracked_workspaces[new_name] = ax.tracked_workspaces.pop(old_name)

            ax.rename_workspace = rename_workspace
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

    def showMessage(self, message: str):
        self.clearView()
        self.messageLabel.setText(message)
        self.messageLabel.show()

    def clearView(self):
        # Remove all existing widgets except the layout
        for i in reversed(range(self.layout.count())):
            widget = self.layout.itemAt(i).widget()
            if widget is not None and widget != self.messageLabel:
                widget.deleteLater()  # Delete the widget

    def getPeakWindowClippingSize(self):
        return int(self.peakWindowClippingSize.field.text())

    def getSmoothingParameter(self):
        return float(self.smoothingSlider.field.text())

    def getLSS(self):
        return self.lssDropdown.currentIndex() == 1

    def getDecreaseParameter(self):
        return self.decreaseParameterDropdown.currentIndex() == 1
