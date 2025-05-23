from abc import abstractmethod
from datetime import datetime, timedelta
from typing import Callable, List, Optional

from qtpy.QtCore import Qt, QTimer, Signal, Slot
from qtpy.QtGui import QColor
from qtpy.QtWidgets import (
    QGridLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QListWidget,
    QMessageBox,
    QPushButton,
    QSlider,
    QStackedLayout,
    QVBoxLayout,
    QWidget,
)

from snapred.backend.dao.RunMetadata import RunMetadata
from snapred.backend.dao.state.RunNumber import RunNumber
from snapred.backend.log.logger import snapredLogger
from snapred.meta.Config import Config
from snapred.meta.decorators.ExceptionToErrLog import ExceptionToErrLog
from snapred.meta.decorators.Resettable import Resettable
from snapred.meta.Enum import StrEnum
from snapred.ui.view.BackendRequestView import BackendRequestView
from snapred.ui.widget.LEDIndicator import LEDIndicator

logger = snapredLogger.getLogger(__name__)


class _RequestViewBase(BackendRequestView):
    liveDataModeChange = Signal(bool)

    def __init__(
        self,
        parent=None,
        getCompatibleMasks: Optional[Callable[[List[str], bool], None]] = None,
        validateRunNumbers: Optional[Callable[[List[str]], None]] = None,
        getLiveMetadata: Optional[Callable[[], RunMetadata]] = None,
    ):
        super(_RequestViewBase, self).__init__(parent=parent)
        self.getCompatibleMasks = getCompatibleMasks
        self.validateRunNumbers = validateRunNumbers
        self.getLiveMetadata = getLiveMetadata

        self.runMetadataCallback: Optional[Callable[[str], RunMetadata]] = None

        self.runNumbers = []
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
        self.retainUnfocusedDataCheckbox.setEnabled(False)
        self.pixelMaskDropdown.setEnabled(False)
        self.convertUnitsDropdown.setEnabled(False)

        # Connect buttons to methods
        self.retainUnfocusedDataCheckbox.checkedChanged.connect(self.convertUnitsDropdown.setEnabled)
        self.liteModeToggle.stateChanged.connect(self._populatePixelMaskDropdown)
        self.liveDataToggle.stateChanged.connect(self.liveDataModeChange)

    def setRunMetadataCallback(self, callback: Callable[[str], RunMetadata]):
        self.runMetadataCallback = callback

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
            self.pixelMaskDropdown.setItems([])
        finally:
            # Re-enable UI elements.
            self.liteModeToggle.setEnabled(True)
            self.pixelMaskDropdown.setEnabled(True)
            self.retainUnfocusedDataCheckbox.setEnabled(True)

    def setInteractive(self, flag: bool):
        # Enable or disable all controls _except_ for the workflow-node buttons (i.e. Continue, Cancel, and etc.).
        self.liteModeToggle.setEnabled(flag)
        self.liveDataToggle.setEnabled(flag)
        self.pixelMaskDropdown.setEnabled(flag)
        self.retainUnfocusedDataCheckbox.setEnabled(flag)
        self.convertUnitsDropdown.setEnabled(flag)

    @abstractmethod
    def verify(self) -> bool:
        # Occurs _after_ continue button is pressed.
        pass

    def useLiteMode(self) -> bool:
        return self.liteModeToggle.getState()

    def keepUnfocused(self) -> bool:
        return self.retainUnfocusedDataCheckbox.isChecked()

    def convertUnitsTo(self) -> str:
        return self.convertUnitsDropdown.currentText()

    def getPixelMasks(self):
        return self.pixelMaskDropdown.checkedItems()

    def liveDataMode(self) -> bool:
        return False

    def liveDataDuration(self) -> timedelta:
        # default value: indicates that all available data should be loaded
        return timedelta(seconds=0)

    def liveDataUpdateInterval(self) -> timedelta:
        # default value: two minute update time
        return timedelta(seconds=120)

    def getRunNumbers(self):
        return self.runNumbers


class _RequestView(_RequestViewBase):
    def __init__(
        self,
        parent=None,
        getCompatibleMasks: Optional[Callable[[List[str], bool], None]] = None,
        validateRunNumbers: Optional[Callable[[List[str]], None]] = None,
        getLiveMetadata: Optional[Callable[[], RunMetadata]] = None,
    ):
        super(_RequestView, self).__init__(
            parent=parent,
            getCompatibleMasks=getCompatibleMasks,
            validateRunNumbers=validateRunNumbers,
            getLiveMetadata=getLiveMetadata,
        )

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

        # Add widgets to layout
        layout_ = self.layout()
        layout_.addLayout(self.runNumberLayout, 0, 0, 1, 2)
        layout_.addWidget(self.runNumberDisplay, 1, 0, 1, 2)
        layout_.addWidget(self.liveDataToggle, 2, 0, 1, 1)
        layout_.addWidget(self.liteModeToggle, 2, 1, 1, 1)
        layout_.addWidget(self.pixelMaskDropdown, 3, 0, 1, 2)
        layout_.addLayout(self.unfocusedDataLayout, 4, 0, 1, 2)

        # Connect buttons to methods
        self.enterRunNumberButton.clicked.connect(self.addRunNumber)
        self.clearButton.clicked.connect(self.clearRunNumbers)

    def _runPreviewText(self, metadata: RunMetadata) -> str:
        return (
            f"{'LIVE: ' if metadata.liveData else ''}"
            + f"{metadata.runNumber},    StateId = {metadata.stateId.hex},    Title = {metadata.runTitle}"
        )

    @Slot()
    def addRunNumber(self):
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
                if self.runMetadataCallback:
                    for runNumber in runNumberList:
                        metadata = self.runMetadataCallback(runNumber)
                        lineText = self._runPreviewText(metadata)
                        self.runNumberDisplay.addItem(lineText)
                self.runNumberInput.clear()
                self._populatePixelMaskDropdown(self.useLiteMode())
        except ValueError as e:
            QMessageBox.warning(self, "Warning", str(e), buttons=QMessageBox.Ok, defaultButton=QMessageBox.Ok)
            self.runNumberInput.clear()

    def parseInputRunNumbers(self) -> List[str]:
        # REMINDER: run numbers are strings.
        #   Be careful parsing them as `int`.
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
                "Reduction run numbers were incorrectly formatted: "
                "please read mantid docs for `IntArrayProperty` on how to format input"
            )

        return [str(num) for num in runs]

    def updateRunNumberList(self):
        self.runNumberDisplay.clear()

    def clearRunNumbers(self):
        self.runNumbers.clear()
        self.runNumberDisplay.clear()
        self.pixelMaskDropdown.setItems([])

    @Slot(str, str)
    def _setRunFeedback(self, stateId: str, runTitle: str):
        if not stateId and not runTitle:
            return
        itemText = f"StateId={stateId}, Title={runTitle}"
        self.runNumberDisplay.addItem(itemText)

    ###
    ### Abstract methods:
    ###

    def verify(self):
        runNumbers = self.runNumbers
        if not runNumbers:
            raise ValueError("Please enter at least one run number.")
        if runNumbers != self.runNumbers:
            raise ValueError("Unexpected issue verifying run numbers. Please clear and re-enter.")
        for runNumber in runNumbers:
            if not runNumber.isdigit():
                pass
        if self.keepUnfocused():
            if self.convertUnitsDropdown.currentIndex() < 0:
                raise ValueError("Please select units to convert to")
        return True

    def setInteractive(self, flag: bool):
        # Enable or disable all controls _except_ for the workflow-node buttons (i.e. Continue, Cancel, and etc.).
        super().setInteractive(flag)
        self.runNumberInput.setEnabled(flag)
        self.enterRunNumberButton.setEnabled(flag)
        self.clearButton.setEnabled(flag)


class _LiveDataView(_RequestViewBase):
    def __init__(
        self,
        parent=None,
        getCompatibleMasks: Optional[Callable[[List[str], bool], None]] = None,
        validateRunNumbers: Optional[Callable[[List[str]], None]] = None,
        getLiveMetadata: Optional[Callable[[], RunMetadata]] = None,
    ):
        super(_LiveDataView, self).__init__(
            parent=parent,
            getCompatibleMasks=getCompatibleMasks,
            validateRunNumbers=validateRunNumbers,
            getLiveMetadata=getLiveMetadata,
        )

        self._liveMetadata = None
        self._reductionStatus = None
        self._lastDisplayedStatus = None
        self._reductionInProgress = False

        # Display and controls specific to `_LiveDataView`:
        self.liveDataIndicator = LEDIndicator()
        #  indicator itself is non-clickable:
        self.liveDataIndicator.setDisabled(False)

        self.liveDataStatus = QLabel()
        self.liveDataSummary = QHBoxLayout()
        self.liveDataSummary.addWidget(self.liveDataIndicator)
        self.liveDataSummary.addWidget(self.liveDataStatus)

        self.durationSlider = QSlider(parent=self, orientation=Qt.Horizontal)
        self.durationSlider.setInvertedAppearance(True)  # Increase from the right
        self.durationSlider.setMinimum(0)
        self.durationSlider.setMaximum(100)  # set a reasonable positive value until first update
        self.duration = self._labeledField(
            "duration: use all available data", self.durationSlider, orientation=Qt.Vertical
        )

        self.updateIntervalSlider = QSlider(parent=self, orientation=Qt.Horizontal)
        self.updateIntervalSlider.setMinimum(Config["liveData.updateIntervalMinimum"])
        self.updateIntervalSlider.setMaximum(Config["liveData.updateIntervalMaximum"])
        defaultUpdateInterval = Config["liveData.updateIntervalDefault"]
        self.updateIntervalSlider.setValue(defaultUpdateInterval)
        self.updateInterval = self._labeledField(
            f"update interval (t0 < {self._formatDuration(defaultUpdateInterval)})",
            self.updateIntervalSlider,
            orientation=Qt.Vertical,
        )
        self._liveDataLayout = QVBoxLayout()
        self._liveDataLayout.addLayout(self.liveDataSummary)
        _sliders = QHBoxLayout()
        _sliders.addWidget(self.duration)
        _sliders.addWidget(self.updateInterval)
        self._liveDataLayout.addLayout(_sliders)

        # Add widgets to layout
        layout_ = self.layout()
        layout_.addLayout(self._liveDataLayout, 0, 0, 1, 2)
        layout_.addWidget(self.liveDataToggle, 1, 0, 1, 1)
        layout_.addWidget(self.liteModeToggle, 1, 1, 1, 1)
        layout_.addWidget(self.pixelMaskDropdown, 2, 0, 1, 2)
        layout_.addLayout(self.unfocusedDataLayout, 3, 0, 1, 2)

        # Automatically update fields depending on time of day, once per second.
        self._timeOfDayUpdateTimer = QTimer(self)
        #   A "chain" update is used here, to prevent runaway-timer issues.
        self._timeOfDayUpdateTimer.setSingleShot(True)
        self._timeOfDayUpdateTimer.setTimerType(Qt.CoarseTimer)
        self._timeOfDayUpdateTimer.setInterval(1000)
        self._timeOfDayUpdateTimer.timeout.connect(self._updateLiveMetadata)

        # Connect signals to slots
        self.durationSlider.valueChanged.connect(self._updateDuration)
        self.updateIntervalSlider.valueChanged.connect(self._updateUpdateInterval)

    @Slot(bool)
    def setReductionInProgress(self, flag: bool):
        self._reductionInProgress = flag

    @Slot(StrEnum)
    def updateStatus(self, status: StrEnum):
        self._reductionStatus = status
        self._updateLiveMetadata()

    @Slot(RunMetadata, StrEnum)
    def updateLiveMetadata(self, data: RunMetadata):
        self._liveMetadata = data
        self._updateLiveMetadata()

    @Slot()
    def _updateLiveMetadata(self):
        # Prevent circular import.
        from snapred.ui.workflow.ReductionWorkflow import ReductionStatus

        if self._timeOfDayUpdateTimer.isActive():
            # This prevents co-incident updates.
            self._timeOfDayUpdateTimer.stop()

        data = self._liveMetadata
        status = self._reductionStatus

        # Special live-data status may override any incoming status already be set by the workflow.
        if data is None:
            status = ReductionStatus.CONNECTING
        elif not data.hasActiveRun():
            status = ReductionStatus.NO_ACTIVE_RUN
        elif not data.beamState():
            status = ReductionStatus.ZERO_PROTON_CHARGE

        # Keep track of the displayed status, so that the flash sequences aren't constantly restarted.
        statusChange = status != self._lastDisplayedStatus
        self._lastDisplayedStatus = status

        TIME_ONLY = "%H:%M:%S"
        TIME_AND_DATE = "%b %d: %H:%M:%S"

        now = datetime.now().astimezone()
        LOCAL_TIMEZONE = now.tzinfo

        startTime = None
        timeFormat = None
        if data is not None:
            # Convert from time in UTC timezone.
            startTime = data.startTime.astimezone(LOCAL_TIMEZONE)
            timeFormat = TIME_ONLY if (now - startTime < timedelta(hours=12)) else TIME_AND_DATE
            self.durationSlider.setEnabled(False)
            self.durationSlider.setMinimum(0)
            self.durationSlider.setMaximum(int(round((now - startTime).total_seconds())))
            self.durationSlider.setEnabled(True)

        match status:
            case ReductionStatus.CONNECTING:
                self.runNumbers = []
                self.liveDataStatus.setText(f"<font size = 4><b>Live data:</b> {status}...</font>")

                # WARNING (i.e. NOT READY) flash
                if statusChange:
                    self.liveDataIndicator.setFlashSequence(((QColor(255, 255, 0),), (0.5, 0.5)))
                    self.liveDataIndicator.setFlash(True)

            case ReductionStatus.NO_ACTIVE_RUN:
                self.runNumbers = []
                self.liveDataStatus.setText(f"<font size = 4><b>Live data:</b> {status}</font>")

                # WARNING (i.e. NOT READY) flash
                if statusChange:
                    self.liveDataIndicator.setFlashSequence(((QColor(255, 255, 0),), (0.4, 0.6)))
                    self.liveDataIndicator.setFlash(True)

            case ReductionStatus.ZERO_PROTON_CHARGE:
                self.runNumbers = []
                self.liveDataStatus.setText(
                    "<p><font size = 4><b>Live data:</b></font>"
                    + "<font size = 4>"
                    + f" running: {data.runNumber}, "
                    + f" since: {startTime.strftime(timeFormat)}</p>"
                    + "</font>"
                    + f"<p><font size = 4> &nbsp;&nbsp; t0(now): {now.strftime(TIME_ONLY)}</font></p>"
                    + f"<p><font size = 4><b>{str(status).upper()}.</b></font></p>"
                )

                # ERROR flash -- proton charge is zero, with a run active.
                if statusChange:
                    self.liveDataIndicator.setFlashSequence(((QColor(255, 0, 0),), (0.1, 0.4)))
                    self.liveDataIndicator.setFlash(True)

            case ReductionStatus.READY:
                liveStateChange = not self.runNumbers or (self.runNumbers[0] != data.runNumber)
                if liveStateChange:
                    self.runNumbers = [data.runNumber]
                    self._populatePixelMaskDropdown(self.useLiteMode())

                # TODO: convert to the local time zone!!

                self.liveDataStatus.setText(
                    "<p><font size = 4><b>Live data:</b></font>"
                    + "<font size = 4>"
                    + f" running: {data.runNumber}, "
                    + f" since: {startTime.strftime(timeFormat)}</p>"
                    + "</font>"
                    + f"<p><font size = 4> &nbsp;&nbsp; t0(now): {now.strftime(TIME_ONLY)}</font></p>"
                    + "<p><font size = 4><b>Ready</b></p>"
                )

                # Solid color (yellow) "ready" indicator.
                if statusChange:
                    self.liveDataIndicator.setColor(QColor(255, 255, 0))
                    self.liveDataIndicator.setChecked(True)

            case ReductionStatus.CALCULATING_NORM:
                self.liveDataStatus.setText(
                    "<p><font size = 4><b>Live data:</b></font>"
                    + "<font size = 4>"
                    + f" running: {data.runNumber}, "
                    + f" since: {startTime.strftime(timeFormat)}</p>"
                    + "</font>"
                    + f"<p><font size = 4> &nbsp;&nbsp; t0(now): {now.strftime(TIME_ONLY)}</font></p>"
                    + "<p><font size = 4><b>Calculating artificial normalization...</b></p>"
                )

                # Green flashing "in progress" indicator.
                if statusChange:
                    self.liveDataIndicator.setFlashSequence(((QColor(0, 255, 0),), (0.4, 0.6)))
                    self.liveDataIndicator.setFlash(True)

            case ReductionStatus.REDUCING_DATA | ReductionStatus.WAITING_TO_LOAD | ReductionStatus.FINALIZING:
                # For the moment, we do not break-out these states:
                #   just display "Reducing data".
                self.liveDataStatus.setText(
                    "<p><font size = 4><b>Live data:</b></font>"
                    + "<font size = 4>"
                    + f" running: {data.runNumber}, "
                    + f" since: {startTime.strftime(timeFormat)}</p>"
                    + "</font>"
                    + f"<p><font size = 4> &nbsp;&nbsp; t0(now): {now.strftime(TIME_ONLY)}</font></p>"
                    + f"<p><font size = 4><b>{ReductionStatus.REDUCING_DATA}</b></p>"
                )

                # Green-blue flashing "in progress" indicator.
                if statusChange:
                    self.liveDataIndicator.setFlashSequence(((QColor(0, 255, 0), QColor(127, 0, 255)), (1.5, 1.5)))
                    self.liveDataIndicator.setFlash(True)

            case ReductionStatus.USER_CANCELLATION:
                self.liveDataStatus.setText(
                    "<p><font size = 4><b>Live data:</b></font>"
                    + "<font size = 4>"
                    + f" running: {data.runNumber}, "
                    + f" since: {startTime.strftime(timeFormat)}</p>"
                    + "</font>"
                    + f"<p><font size = 4> &nbsp;&nbsp; t0(now): {now.strftime(TIME_ONLY)}</font></p>"
                    + "<p><font size = 4><b>Cancelling workflow, please wait...</b></p>"
                )

                # WARNING (i.e. NOT READY) flash
                if statusChange:
                    self.liveDataIndicator.setFlashSequence(((QColor(255, 255, 0),), (0.4, 0.6)))
                    self.liveDataIndicator.setFlash(True)

            case _:
                raise RuntimeError(f"`ReductionRequestView`: unrecognized live-data status {status}")

        # Automatically update any fields depending on time of day, once per second:
        # -- chain single shot.
        self._timeOfDayUpdateTimer.start()

    def hideEvent(self, event):  # noqa: ARG002
        if self._timeOfDayUpdateTimer.isActive():
            self._timeOfDayUpdateTimer.stop()

    def showEvent(self, event):  # noqa: ARG002
        # Automatically update any fields depending on time of day, once per second:
        self._updateLiveMetadata()

    def _formatDuration(self, seconds: int) -> str:
        # Format a time duration.
        # Note that `timedelta` retains `days` and `seconds` internally,
        #   however days will be prefixed, when appropriate, by `timedelta.__str__`.
        dt = timedelta(seconds=seconds)
        units = "h" if dt.seconds >= 60**2 else "m" if dt.seconds >= 60 else "s"
        return f"{dt}{units}"

    @Slot(int)
    def _updateDuration(self, seconds: int):
        text = f"duration ({self._formatDuration(seconds)} < t0)" if seconds > 0 else "duration: use all available data"
        self.duration.setLabelText(text)

    @Slot(int)
    def _updateUpdateInterval(self, seconds: int):
        self.updateInterval.setLabelText(f"update interval (t0 < {self._formatDuration(seconds)})")

    ###
    ### Abstract methods:
    ###

    def verify(self) -> bool:
        if self._liveMetadata is None:
            raise RuntimeError("usage error: `verify` called before first call to `updateLiveMetadata`")

        # -- For completeness, these checks should also be here, but they are redundant: --
        if not self._liveMetadata.hasActiveRun():
            raise ValueError("No run is active.")
        if not self._liveMetadata.beamState():
            raise ValueError("The proton charge is zero")
        # -- end: redundant checks --

        if self.keepUnfocused():
            if self.convertUnitsDropdown.currentIndex() < 0:
                raise ValueError("Please select units to convert to")
        return True

    def setInteractive(self, flag: bool):
        # Enable or disable all controls _except_ for the workflow-node buttons (i.e. Continue, Cancel, and etc.).
        super().setInteractive(flag)

    def liveDataMode(self) -> bool:
        return True

    def liveDataDuration(self) -> timedelta:
        _value = self.durationSlider.value()
        # a duration of seconds=0 indicates that all of the available data should be loaded
        return timedelta(seconds=_value)

    def liveDataUpdateInterval(self) -> timedelta:
        _value = self.updateIntervalSlider.value()
        return timedelta(seconds=_value)


@Resettable
class ReductionRequestView(QWidget):
    liveDataModeChange = Signal(bool)

    def __init__(
        self,
        parent=None,
        getCompatibleMasks: Optional[Callable[[List[str], bool], None]] = None,
        validateRunNumbers: Optional[Callable[[List[str]], None]] = None,
        getLiveMetadata: Optional[Callable[[], RunMetadata]] = None,
    ):
        super(ReductionRequestView, self).__init__(parent=parent)

        self._requestView = _RequestView(
            parent=parent,
            getCompatibleMasks=getCompatibleMasks,
            validateRunNumbers=validateRunNumbers,
            getLiveMetadata=getLiveMetadata,
        )
        self._liveDataView = _LiveDataView(
            parent=parent,
            getCompatibleMasks=getCompatibleMasks,
            validateRunNumbers=validateRunNumbers,
            getLiveMetadata=getLiveMetadata,
        )

        self.setLayout(QGridLayout())
        self._stackedLayout = QStackedLayout()
        self._stackedLayout.addWidget(self._requestView)
        self._stackedLayout.addWidget(self._liveDataView)
        self.layout().addLayout(self._stackedLayout, 0, 0)

        # Connect signals to slots
        self._requestView.liveDataModeChange.connect(self.liveDataModeChange)
        self._liveDataView.liveDataModeChange.connect(self.liveDataModeChange)
        self.liveDataModeChange.connect(self._setLiveDataMode)

    @Slot(bool)
    def _setLiveDataMode(self, flag: bool):
        self._stackedLayout.setCurrentWidget(self._liveDataView if flag else self._requestView)
        view = self._stackedLayout.currentWidget()
        view.liveDataToggle.toggle.setEnabled(False)

        if flag:
            view.liveDataToggle.setState(True)
        else:
            view.liveDataToggle.setState(False)

        view.liveDataToggle.toggle.setEnabled(True)

    def updateRunFeedback(self, stateId: str, runTitle: str):
        if not self.liveDataMode():
            self._requestView.updateRunFeedback(stateId, runTitle)

    @Slot(RunMetadata)
    def updateLiveMetadata(self, data: RunMetadata):
        if self.liveDataMode():
            self._liveDataView.updateLiveMetadata(data)

    @Slot(StrEnum)
    def updateStatus(self, status: StrEnum):
        if self.liveDataMode():
            self._liveDataView.updateStatus(status)

    @Slot(bool)
    def setReductionInProgress(self, flag: bool):
        if self.liveDataMode():
            self._liveDataView.setReductionInProgress(flag)

    @Slot(bool)
    def setLiveDataToggleEnabled(self, flag: bool):
        self._requestView.liveDataToggle.setEnabled(flag)
        self._liveDataView.liveDataToggle.setEnabled(flag)

    ###
    ### Abstract methods: ??? derive methods separately?
    ###

    def verify(self) -> bool:
        return self._stackedLayout.currentWidget().verify()

    def setInteractive(self, flag: bool):
        self._stackedLayout.currentWidget().setInteractive(flag)

    def useLiteMode(self) -> bool:
        return self._stackedLayout.currentWidget().useLiteMode()

    def keepUnfocused(self) -> bool:
        return self._stackedLayout.currentWidget().keepUnfocused()

    def convertUnitsTo(self) -> str:
        return self._stackedLayout.currentWidget().convertUnitsTo()

    def liveDataMode(self) -> bool:
        return self._stackedLayout.currentWidget().liveDataMode()

    def liveDataDuration(self) -> timedelta:
        return self._stackedLayout.currentWidget().liveDataDuration()

    def liveDataUpdateInterval(self) -> timedelta:
        return self._stackedLayout.currentWidget().liveDataUpdateInterval()

    def getRunNumbers(self) -> List[str]:
        return self._stackedLayout.currentWidget().getRunNumbers()

    def getPixelMasks(self) -> List[str]:
        return self._stackedLayout.currentWidget().getPixelMasks()
