import matplotlib
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from PyQt5.QtCore import Qt, pyqtSignal
from PyQt5.QtWidgets import QComboBox, QGridLayout, QLabel, QLineEdit, QSlider, QWidget

from snapred.ui.widget.JsonFormList import JsonFormList
from snapred.ui.widget.LabeledField import LabeledField


class SpecifyNormalizationCalibrationView(QWidget):
    signalRunNumberUpdate = pyqtSignal(str)
    signalSampleUpdate = pyqtSignal(int)
    signalGroupingUpdate = pyqtSignal(int)
    signalBackgroundRunNumberUpdate = pyqtSignal(str)
    signalCalibrantUpdate = pyqtSignal(int)
    signalSmoothingValueChanged = pyqtSignal(float)
    signalWorkspacesUpdate = pyqtSignal(str, str)

    def __init__(self, name, jsonSchemaMap, samples=[], groups=[], calibrantSamples=[], parent=None):
        super().__init__(parent)
        self._jsonFormList = JsonFormList(name, jsonSchemaMap, parent=parent)

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
        self.sampleDropDown.addItem("Select Sample")
        self.sampleDropDown.addItems(samples)
        self.sampleDropDown.model().item(0).setEnabled(False)
        self.signalSampleUpdate.connect(self._updateSample)

        self.groupingDropDown = QComboBox()
        self.groupingDropDown.setEnabled(True)
        self.groupingDropDown.addItem("Select Grouping")
        self.groupingDropDown.addItems(groups)
        self.groupingDropDown.model().item(0).setEnabled(False)
        self.signalGroupingUpdate.connect(self._updateGrouping)
        self.groupingDropDown.currentIndexChanged.connect(self.onGroupingChanged)

        self.calibrantDropDown = QComboBox()
        self.calibrantDropDown.setEnabled(False)
        self.calibrantDropDown.addItem("Select Calibrant")
        self.calibrantDropDown.addItems(calibrantSamples)
        self.calibrantDropDown.model().item(0).setEnabled(False)
        self.signalCalibrantUpdate.connect(self._updateCalibrant)

        self.smoothingSlider = QSlider(Qt.Horizontal)
        self.smoothingSlider.setMinimum(0)
        self.smoothingSlider.setMaximum(100)
        self.smoothingSlider.setValue(0)
        self.smoothingSlider.setTickInterval(1)
        self.smoothingSlider.setSingleStep(1)
        self.smoothingSlider.valueChanged.connect(self.onSmoothingValueChanged)

        self.layout.addWidget(self.canvas)
        self.layout.addWidget(self.fieldRunNumber)
        self.layout.addWidget(self.fieldBackgroundRunNumber)
        self.layout.addWidget(LabeledField("Smoothing Parameter:", self.smoothingSlider, self))
        self.layout.addWidget(LabeledField("Sample :", self.sampleDropDown, self))
        self.layout.addWidget(LabeledField("Grouping File :", self.groupingDropDown, self))
        self.layout.addWidget(LabeledField("Calibrant :", self.calibrantDropDown, self))

    def _updateRunNumber(self, runNumber):
        self.fieldRunNumber.setText(runNumber)

    def updateRunNumber(self, runNumber):
        self.signalRunNumberUpdate.emit(runNumber)

    def _updateBackgroundRunNumber(self, backgroundRunNumber):
        self.fieldBackgroundRunNumber.setText(backgroundRunNumber)

    def updateBackgroundRunNumber(self, backgroundRunNumber):
        self.signalBackgroundRunNumberUpdate.emit(backgroundRunNumber)

    def _updateSample(self, sampleIndex):
        self.sampleDropDown.setCurrentIndex(sampleIndex)

    def updateSample(self, sampleIndex):
        self.signalSampleUpdate.emit(sampleIndex)

    def _updateGrouping(self, groupingIndex):
        self.groupingDropDown.setCurrentIndex(groupingIndex)

    def updateGrouping(self, groupingIndex):
        self.signalGroupingUpdate.emit(groupingIndex)

    def _updateCalibrant(self, calibrantIndex):
        self.calibrantDropDown.setCurrentIndex(calibrantIndex)

    def updateCalibrant(self, calibrantIndex):
        self.signalCalibrantUpdate.emit(calibrantIndex)

    def onSmoothingValueChanged(self, value):
        self.smoothingValue = value / 100.0
        self.signalSmoothingValueChanged.emit(self.smoothingValue)

    def onGroupingChanged(self, index):
        if index > 0:
            self.groupingDropDown.currentText()
            # self.update(groupingSchema)

    # def updateWorkspaces(self, focusWorkspace, smoothedWorkspace):
    #     self.focusWorkspace = focusWorkspace
    #     self.smoothedWorkspace = smoothedWorkspace
    #     self._updateGraphs(focusWorkspace, smoothedWorkspace)

    # def _updateGraphs(self, groupingSchema, numGroups):
    #     # Clear the existing figure
    #     self.figure.clear()

    #     self.groupinSchema = groupingSchema
    #     # Define a layout for the subplots
    #     layout = [["ws1_plot" + str(i) for i in range(numGroups)], ["ws2_plot" + str(i) for i in range(numGroups)]]

    #     # Create subplots based on the layout
    #     ax_dict = self.figure.subplot_mosaic(layout, subplot_kw={"projection": "mantid"})

    #     # Loop through the number of groups and plot each workspace
    #     for i in range(numGroups):
    #         # Retrieve workspaces from the lists
    #         focusWorkspace = self.focusWorkspaces[i]
    #         clonedWorkspace = self.clonedWorkspaces[i]

    # You might need to extract the data from the workspace depending on how it's stored
    # This is a placeholder for that process
    # focusData = extract_data_from_workspace(focusWorkspace)
    # clonedData = extract_data_from_workspace(clonedWorkspace)

    #     # Plot data on respective axes
    #     ax_dict["ws1_plot" + str(i)].plot(focusWorkspace.readY(0), label="Focus Workspace")
    #     ax_dict["ws2_plot" + str(i)].plot(clonedWorkspace.readY(0), label="Cloned Workspace")

    #     # Optionally, you can set labels, titles, etc.
    #     ax_dict["ws1_plot" + str(i)].set_title(f"Focus Workspace {i}")
    #     ax_dict["ws2_plot" + str(i)].set_title(f"Cloned Workspace {i}")
    #     ax_dict["ws1_plot" + str(i)].legend()
    #     ax_dict["ws2_plot" + str(i)].legend()

    # # Update the canvas to reflect new plots
    # self.canvas.draw()

    # # Determine the number of graphs based on the grouping scheme
    # if groupingScheme == "All":
    #     numGroups = load_grouping_scheme('/path/to/SNAPFocGroup_All.lite.hdf')
    # elif groupingScheme == "Bank":
    #     numGroups = load_grouping_scheme('/path/to/SNAPFocGroup_Bank.lite.hdf')
    # elif groupingScheme == "Column":
    #     numGroups = load_grouping_scheme('/path/to/SNAPFocGroup_Column.lite.hdf')
    # else:
    #     return  # Handle invalid grouping scheme

    # numGraphs = numGroups * 2  # Since there are two workspaces

    # # Create subplots and plot data
    # for i in range(numGraphs):
    #     ax = self.figure.add_subplot(numGroups, 2, i + 1)
    #     group_index = i // 2
    #     workspace_index = i % 2
    #     workspace_name = self.focusWorkspaces[workspace_index] if workspace_index <
    #     len(self.focusWorkspaces) else None

    #     if workspace_name:
    #         spectra = self.get_spectra_data(workspace_name, group_index)
    #         ax.plot(spectra)  # Plot the spectra data
    #         # Additional plot formatting here

    # self.canvas.draw()
