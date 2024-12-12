from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Callable, List, Optional

from qtpy.QtCore import Qt, QSize, Signal, Slot
from qtpy.QtGui import QColor
from qtpy.QtWidgets import (
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QListWidget,
    QMessageBox,
    QPushButton,
    QSlider,
    QStackedLayout,
    QVBoxLayout,
)

from snapred.backend.dao.LiveMetadata import LiveMetadata
from snapred.backend.dao.state.RunNumber import RunNumber
from snapred.backend.log.logger import snapredLogger
from snapred.meta.decorators.Resettable import Resettable
from snapred.meta.decorators.ExceptionToErrLog import ExceptionToErrLog
from snapred.ui.view.BackendRequestView import BackendRequestView
from snapred.ui.widget.Toggle import Toggle
from snapred.ui.widget.LEDIndicator import LEDIndicator

logger = snapredLogger.getLogger(__name__)


class _RequestViewBase(BackendRequestView):

    @abstractmethod
    def useLiteMode(self) -> bool:
        pass

    @abstractmethod
    def keepUnfocused(self) -> bool:
        pass

    @abstractmethod
    def convertUnitsTo(self) -> str:
        pass

    @abstractmethod
    def liveDataMode(self) -> bool:
        pass

    @abstractmethod
    def getRunNumbers(self) -> List[str]:
        pass

    @abstractmethod
    def getPixelMasks(self) -> List[str]:
        pass


class _RequestView(_RequestViewBase):
    liveDataModeChange = Signal(bool)
       
    def __init__(
        self,
        parent=None,
        getCompatibleMasks: Optional[Callable[[List[str], bool], None]] = None,
        validateRunNumbers: Optional[Callable[[List[str]], None]] = None,
        getLiveMetadata: Optional[Callable[[], LiveMetadata]] = None,
    ):
        super(_RequestView, self).__init__(parent=parent)
        self.getCompatibleMasks = getCompatibleMasks
        self.validateRunNumbers = validateRunNumbers
        self.getLiveMetadata = getLiveMetadata

        self.runNumbers = []
        self.pixelMaskDropdown = self._multiSelectDropDown("Select Pixel Mask(s)", [])

        # Horizontal layout for run number input and button
        self.runNumberLayout = QHBoxLayout()
        self.runNumberInput = QLineEdit()
        self.runNumberInput.returnPressed.connect(self.addRunNumber)
        self.enterRunNumberButton = QPushButton("Enter Run Number")
        self.clearButton = QPushButton("Clear")
        self.runNumberButtonLayout = QVBoxLayout()
        self.runNumberButtonLayout.addWidget(self.enterRunNumberButton)
        self.runNumberButtonLayout.addWidget(self.clearButton)

        self.runNumberLayout.addWidget(self.runNumberInput)
        self.runNumberLayout.addLayout(self.runNumberButtonLayout)

        # Run number display
        self.runNumberDisplay = QListWidget()
        self.runNumberDisplay.setSortingEnabled(False)

        # Lite mode toggle, pixel masks dropdown, and retain unfocused data checkbox
        self.liteModeToggle = self._labeledToggle("Lite Mode", state=True)
        self.liteModeToggle.toggle.setObjectName("liteModeToggle")

        self.retainUnfocusedDataCheckbox = self._labeledCheckBox("Retain Unfocused Data")
        self.convertUnitsDropdown = self._sampleDropDown(
            "Convert Units", ["TOF", "dSpacing", "Wavelength", "MomentumTransfer"]
        )
        self.convertUnitsDropdown.setCurrentIndex(1)
        
        self.unfocusedDataLayout = QHBoxLayout()
        self.unfocusedDataLayout.addWidget(self.retainUnfocusedDataCheckbox, 1)
        self.unfocusedDataLayout.addWidget(self.convertUnitsDropdown, 2)
        
        # live-data toggle
        self.liveDataToggle = self._labeledToggle("Live data", state=False)
        self.liveDataToggle.toggle.setObjectName("liveDataToggle")

        # Set field properties
        self.liteModeToggle.setEnabled(False)
        self.retainUnfocusedDataCheckbox.setEnabled(False)
        self.pixelMaskDropdown.setEnabled(False)
        self.convertUnitsDropdown.setEnabled(False)

        # Add widgets to layout
        _layout = self.layout()
        _layout.addLayout(self.runNumberLayout, 0, 0, 1, 2)
        _layout.addWidget(self.runNumberDisplay, 1, 0, 1, 2)
        _layout.addWidget(self.liveDataToggle, 2, 0, 1, 1)
        _layout.addWidget(self.liteModeToggle, 2, 1, 1, 1)
        _layout.addWidget(self.pixelMaskDropdown, 3, 0, 1, 2)
        _layout.addLayout(self.unfocusedDataLayout, 4, 0, 1, 2)

        # Connect buttons to methods
        self.enterRunNumberButton.clicked.connect(self.addRunNumber)
        self.clearButton.clicked.connect(self.clearRunNumbers)
        self.retainUnfocusedDataCheckbox.checkedChanged.connect(self.convertUnitsDropdown.setEnabled)
        self.liteModeToggle.stateChanged.connect(self._populatePixelMaskDropdown)
        self.liveDataToggle.stateChanged.connect(lambda flag: self.liveDataModeChange.emit(flag))

    @Slot()
    def addRunNumber(self):
        # TODO: FIX THIS!
        #   We're not inside the SNAPResponseHandler here, so we can't just throw a `ValueError`.
        try:
            runNumberList = self.parseInputRunNumbers()
            if runNumberList is not None:
                # remove duplicates
                noDuplicates = set(self.runNumbers)
                noDuplicates.update(runNumberList)
                noDuplicates = list(noDuplicates)
                if self.validateRunNumbers is not None:
                    self.validateRunNumbers(noDuplicates)
                self.runNumbers = noDuplicates
                self.updateRunNumberList()
                self.runNumberInput.clear()
                self._populatePixelMaskDropdown(self.useLiteMode())
        except ValueError as e:
            QMessageBox.warning(self, "Warning", str(e), buttons=QMessageBox.Ok, defaultButton=QMessageBox.Ok)
            self.runNumberInput.clear()

    @ExceptionToErrLog
    @Slot(bool)
    def _populatePixelMaskDropdown(self, useLiteMode: bool):
        runNumbers = self.getRunNumbers()

        self.liteModeToggle.setEnabled(False)
        self.pixelMaskDropdown.setEnabled(False)
        self.retainUnfocusedDataCheckbox.setEnabled(False)

        try:
            # Get compatible masks for the current reduction state.
            masks = []
            if self.getCompatibleMasks:
                masks = self.getCompatibleMasks(runNumbers, useLiteMode)

            # Populate the dropdown with the mask names.
            self.pixelMaskDropdown.setItems(masks)
        except Exception as e:  # noqa: BLE001
            print(f"Error retrieving compatible masks: {e}")
            self._reductionRequestView.pixelMaskDropdown.setItems([])
        finally:
            # Re-enable UI elements.
            self.liteModeToggle.setEnabled(True)
            self.pixelMaskDropdown.setEnabled(True)
            self.retainUnfocusedDataCheckbox.setEnabled(True)

    def parseInputRunNumbers(self) -> List[str]:
        # WARNING: run numbers are strings.
        #   For now, it's OK to parse them as integer, but they should not be passed around that way.
        try:
            runs, errors = RunNumber.runsFromIntArrayProperty(self.runNumberInput.text(), False)

            if len(errors) > 0:
                messageBox = QMessageBox(
                    QMessageBox.Warning,
                    "Warning",
                    "There are issues with some run(s)",
                    QMessageBox.Ok,
                    self,
                )
                formattedErrors = "\n\n".join([error[1] for error in errors])
                messageBox.setDetailedText(f"{formattedErrors}")
                messageBox.exec()
        except Exception:  # noqa BLE001
            raise ValueError(
                "Bad input was given for Reduction runs,"
                "please read mantid docs for IntArrayProperty on how to format input"
            )

        return [str(num) for num in runs]

    def updateRunNumberList(self):
        self.runNumberDisplay.clear()
        self.runNumberDisplay.addItems(self.runNumbers)

    def clearRunNumbers(self):
        self.runNumbers.clear()
        self.runNumberDisplay.clear()
        self.pixelMaskDropdown.setItems([])

    ###
    ### Abstract methods:
    ###

    def verify(self):
        runNumbers = self.runNumbers
        if not runNumbers:
            raise ValueError("Please enter at least one run number.")
        if runNumbers != self.runNumbers:
            raise ValueError("Unexpected issue verifying run numbers.  Please clear and re-enter.")
        for runNumber in runNumbers:
            if not runNumber.isdigit():
                raise ValueError(
                    "Please enter a valid run number or list of run numbers. (e.g. 46680, 46685, 46686, etc...)"
                )
        if self.keepUnfocused():
            if self.convertUnitsDropdown.currentIndex() < 0:
                raise ValueError("Please select units to convert to")
        return True
    
    def useLiteMode(self) -> bool:
        return self.liteModeToggle.getState()

    def keepUnfocused(self) -> bool:
        return self.retainUnfocusedDataCheckbox.isChecked() 

    def convertUnitsTo(self) -> str:
        return self.convertUnitsDropdown.currentText()

    def liveDataMode(self) -> bool:
        return self.liveDataToggle.getState()

    def getRunNumbers(self):
        return self.runNumbers

    def getPixelMasks(self):
        return self.pixelMaskDropdown.checkedItems()


class _LiveDataView(_RequestViewBase):
    liveDataModeChange = Signal(bool)
        
    def __init__(
        self,
        parent=None,
        getCompatibleMasks: Optional[Callable[[List[str], bool], None]] = None,
        validateRunNumbers: Optional[Callable[[List[str]], None]] = None,
        getLiveMetadata: Optional[Callable[[], LiveMetadata]] = None,
    ):
        super(_LiveDataView, self).__init__(parent=parent)
        self.getCompatibleMasks = getCompatibleMasks
        self.validateRunNumbers = validateRunNumbers
        self.getLiveMetadata = getLiveMetadata

        self.runNumbers = []
        
        # Display and controls specific to `_LiveDataView`:
        self.liveDataIndicator = LEDIndicator()
        #  indicator itself is non-clickable:
        self.liveDataIndicator.setDisabled(False)
        
        self.liveDataStatus = QLabel()
        self.liveDataSummary = QHBoxLayout()
        self.liveDataSummary.addWidget(self.liveDataIndicator)
        self.liveDataSummary.addWidget(self.liveDataStatus)
        
        self.liveDataDuration = self._labeledField(
            "duration (< t0: s)",
            QSlider(parent=self, orientation=Qt.Horizontal),
            orientation=Qt.Vertical
        )
        self.liveDataDuration.field.setInvertedAppearance(True) # Increase from the right
        self.liveDataUpdateInterval = self._labeledField(
            "update interval (> t0: s)",
            QSlider(parent=self, orientation=Qt.Horizontal),
            orientation=Qt.Vertical
        )
        self._liveDataLayout = QVBoxLayout()
        self._liveDataLayout.addLayout(self.liveDataSummary)
        _sliders = QHBoxLayout()
        _sliders.addWidget(self.liveDataDuration)
        _sliders.addWidget(self.liveDataUpdateInterval)
        self._liveDataLayout.addLayout(_sliders)
        
        # Display and controls in common with  `_RequestView`.
        self.pixelMaskDropdown = self._multiSelectDropDown("Select Pixel Mask(s)", [])

        # Lite mode toggle, pixel masks dropdown, and retain unfocused data checkbox
        self.liteModeToggle = self._labeledToggle("Lite Mode", state=True)
        self.liteModeToggle.toggle.setObjectName("liteModeToggle")

        self.retainUnfocusedDataCheckbox = self._labeledCheckBox("Retain Unfocused Data")
        self.convertUnitsDropdown = self._sampleDropDown(
            "Convert Units", ["TOF", "dSpacing", "Wavelength", "MomentumTransfer"]
        )
        self.convertUnitsDropdown.setCurrentIndex(1)
        
        self.unfocusedDataLayout = QHBoxLayout()
        self.unfocusedDataLayout.addWidget(self.retainUnfocusedDataCheckbox, 1)
        self.unfocusedDataLayout.addWidget(self.convertUnitsDropdown, 2)
        
        # live-data toggle
        self.liveDataToggle = self._labeledToggle("Live data", state=False)
        self.liveDataToggle.toggle.setObjectName("liveDataToggle")

        # Set field properties
        self.liteModeToggle.setEnabled(False)
        self.pixelMaskDropdown.setEnabled(False)
        self.retainUnfocusedDataCheckbox.setEnabled(False)
        self.convertUnitsDropdown.setEnabled(False)

        # Add widgets to layout
        _layout = self.layout()
        _layout.addLayout(self._liveDataLayout, 0, 0, 1, 2)
        _layout.addWidget(self.liveDataToggle, 1, 0, 1, 1)
        _layout.addWidget(self.liteModeToggle, 1, 1, 1, 1)
        _layout.addWidget(self.pixelMaskDropdown, 2, 0, 1, 2)
        _layout.addLayout(self.unfocusedDataLayout, 3, 0, 1, 2)

        # Connect buttons to methods
        self.retainUnfocusedDataCheckbox.checkedChanged.connect(self.convertUnitsDropdown.setEnabled)
        self.liteModeToggle.stateChanged.connect(self._populatePixelMaskDropdown)
        self.liveDataToggle.stateChanged.connect(lambda flag: self.liveDataModeChange.emit(flag))

    @Slot(LiveMetadata)
    def updateLiveMetadata(self, data: LiveMetadata):
        TIME_ONLY_UTC = "%H:%m:%S (utc)"
        TIME_AND_DATE_UTC = "%b %d: %H:%m:%S (utc)"
        
        if data.hasActiveRun():
            if data.beamState():
                liveStateChange = not self.runNumbers or (self.runNumbers[0] != data.runNumber)
                if liveStateChange:
                    self.runNumbers = [data.runNumber]
                    self._populatePixelMaskDropdown(self.useLiteMode())

                # TODO: convert to local time zone
                self.startTime = data.startTime

                # Update the status display
                timeFormat = TIME_ONLY_UTC if (datetime.utcnow() - data.startTime < timedelta(hours=12)) else TIME_AND_DATE_UTC
                self.liveDataStatus.setText(
                    "<p><font size = 4><b>Live data:</font>" 
                    + "<font size = 3>"
                    + f" running: {data.runNumber}, "
                    + f" since: {data.startTime.strftime(timeFormat)}</p>"
                    + "</font>"
                    + f"<p><font size = 3> &nbsp;&nbsp; t0(now): {datetime.utcnow().strftime(TIME_ONLY_UTC)}</font></p>"
                )

                self.liveDataIndicator.setColor(QColor(255, 255, 0))
                self.liveDataIndicator.setChecked(True)
            else:
                self.runNumbers = []
                timeFormat = TIME_ONLY_UTC if (datetime.utcnow() - data.startTime < timedelta(hours=12)) else TIME_AND_DATE_UTC
                self.liveDataStatus.setText(
                    "<p><font size = 4><b>Live data:</b></font>" 
                    + "<font size = 3>"
                    + f" running: {data.runNumber}, "
                    + f" since: {data.startTime.strftime(timeFormat)}</p>"
                    + "</font>"
                    + f"<p><font size = 3> &nbsp;&nbsp; t0(now): {datetime.utcnow().strftime(TIME_ONLY_UTC)}</font></p>"
                    + "<p><font size = 4><b>BEAM is DOWN.</b></font></p>"
                )
                # ERROR flash -- beam is down with a run active.
                self.liveDataIndicator.setFlashSequence(((QColor(255, 0, 0),), (0.1, 0.4)))
                self.liveDataIndicator.setFlash(True)
            
        else:
            self.runNumbers = []
            self.liveDataStatus.setText(
                "<font size = 4><b>Live data:</b> no run is active</font>" 
            )
            # WARNING flash
            self.liveDataIndicator.setFlashSequence(((QColor(255, 255, 0),), (0.1, 0.4)))
            self.liveDataIndicator.setFlash(True)
                
    @ExceptionToErrLog
    @Slot(bool)
    def _populatePixelMaskDropdown(self, useLiteMode: bool):
        runNumbers = self.getRunNumbers()

        self.liteModeToggle.setEnabled(False)
        self.pixelMaskDropdown.setEnabled(False)
        self.retainUnfocusedDataCheckbox.setEnabled(False)

        try:
            # Get compatible masks for the current reduction state.
            masks = []
            if self.getCompatibleMasks:
                masks = self.getCompatibleMasks(runNumbers, useLiteMode)

            # Populate the dropdown with the mask names.
            self.pixelMaskDropdown.setItems(masks)
        except Exception as e:  # noqa: BLE001
            print(f"Error retrieving compatible masks: {e}")
            self._reductionRequestView.pixelMaskDropdown.setItems([])
        finally:
            # Re-enable UI elements.
            self.liteModeToggle.setEnabled(True)
            self.pixelMaskDropdown.setEnabled(True)
            self.retainUnfocusedDataCheckbox.setEnabled(True)

    ###
    ### Abstract methods:
    ###

    def verify(self):
        if not self.liveMetadata.hasActiveRun():
            raise ValueError("No live-data acquisition is active.")
        for runNumber in self.runNumbers:
            if not runNumber.isdigit():
                raise ValueError("Unexpected run number format")
        if self.keepUnfocused():
            if self.convertUnitsDropdown.currentIndex() < 0:
                raise ValueError("Please select units to convert to")
        return True
    
    def useLiteMode(self) -> bool:
        return self.liteModeToggle.getState()

    def keepUnfocused(self) -> bool:
        return self.retainUnfocusedDataCheckbox.isChecked() 

    def convertUnitsTo(self) -> str:
        return self.convertUnitsDropdown.currentText()

    def liveDataMode(self) -> bool:
        return self.liveDataToggle.getState()

    def getRunNumbers(self) -> List[str]:
        return self.runNumbers

    def getPixelMasks(self) -> List[str]:
        return self.pixelMaskDropdown.checkedItems()


@Resettable
class ReductionRequestView(_RequestViewBase):

    def __init__(
        self,
        parent=None,
        getCompatibleMasks: Optional[Callable[[List[str], bool], None]] = None,
        validateRunNumbers: Optional[Callable[[List[str]], None]] = None,
        getLiveMetadata: Optional[Callable[[], LiveMetadata]] = None,
    ):
        super(ReductionRequestView, self).__init__(parent=parent)
        self.getCompatibleMasks = getCompatibleMasks
        self.validateRunNumbers = validateRunNumbers
        self.getLiveMetadata = getLiveMetadata

        self._requestView = _RequestView(
            parent=parent,
            getCompatibleMasks=getCompatibleMasks,
            validateRunNumbers=validateRunNumbers,
            getLiveMetadata=getLiveMetadata
        )
        self._liveDataView = _LiveDataView(
            parent=parent,
            getCompatibleMasks=getCompatibleMasks,
            validateRunNumbers=validateRunNumbers,
            getLiveMetadata=getLiveMetadata
        )

        self._stackedLayout = QStackedLayout()
        self._stackedLayout.addWidget(self._requestView)
        self._stackedLayout.addWidget(self._liveDataView)
        self.layout().addLayout(self._stackedLayout, 0, 0)

        # Connect signals to slots
        self._stackedLayout.currentChanged.connect(self._changeLiveDataMode)
        self._requestView.liveDataModeChange.connect(self._liveDataModeChange)
        self._liveDataView.liveDataModeChange.connect(self._liveDataModeChange)

    @Slot(bool)
    def _liveDataModeChange(self, flag: bool):
        if flag:
            self._stackedLayout.setCurrentWidget(self._liveDataView)  
        else:
            self._stackedLayout.setCurrentWidget(self._requestView)

    @Slot()
    def _changeLiveDataMode(self):
        view = self._stackedLayout.currentWidget()
        view.liveDataToggle.toggle.setEnabled(False)
        if view == self._requestView:
            view.liveDataToggle.setState(False)
        elif view == self._liveDataView:
            view.liveDataToggle.setState(True)
            
            # *** DEBUG ***
            if self.getLiveMetadata:
                _data = self.getLiveMetadata()
                view.updateLiveMetadata(_data)
        view.liveDataToggle.toggle.setEnabled(True)
                
    ###
    ### Abstract methods:
    ###
    def verify(self):
        self._stackedLayout.currentWidget().verify()
    
    def useLiteMode(self) -> bool:
        return self._stackedLayout.currentWidget().useLiteMode()

    def keepUnfocused(self) -> bool:
        return self._stackedLayout.currentWidget().keepUnfocused()

    def convertUnitsTo(self) -> str:
        return self._stackedLayout.currentWidget().convertUnitsTo()

    def liveDataMode(self) -> bool:
        return self._stackedLayout.currentWidget().liveDataMode()

    def getRunNumbers(self) -> List[str]:
        return self._stackedLayout.currentWidget().getRunNumbers()

    def getPixelMasks(self) -> List[str]:
        return self._stackedLayout.currentWidget().getPixelMasks()

