import logging
import threading

from qtpy.QtCore import Signal, Slot
from qtpy.QtWidgets import QMessageBox, QWidget

from snapred.backend.dao.SNAPResponse import ResponseCode, SNAPResponse
from snapred.backend.error.ContinueWarning import ContinueWarning
from snapred.backend.error.LiveDataState import LiveDataState
from snapred.backend.error.RecoverableException import RecoverableException
from snapred.backend.error.UserCancellation import UserCancellation
from snapred.backend.log.logger import snapredLogger
from snapred.ui.view.InitializeStateCheckView import InitializationMenu

logger = snapredLogger.getLogger(__name__)


class SNAPResponseHandler(QWidget):
    signal = Signal(object)
    signalWarning = Signal(str)
    continueAnyway = Signal(object)
    resetWorkflow = Signal()
    userCancellation = Signal(object)
    liveDataStateTransition = Signal(object)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.signal.connect(self._handle)
        self.signalWarning.connect(self._handleWarning)

    def handle(self, result):
        self.signal.emit(result)

    @Slot(SNAPResponse)
    def _handle(self, result):
        # if no complications, do nothing here (program will continue)
        # if errors, do nothing here (program will halt)
        # if a continue warning was raised, receive what user selected
        # if the user selected to continue anyway, then emit the signal to continue anyway

        if threading.current_thread() is threading.main_thread():
            self._handleComplications(result.code, result.message)
        else:
            self.rethrow(result)

    @staticmethod
    def _isErrorCode(code):
        return code >= ResponseCode.ERROR

    @staticmethod
    def _isRecoverableError(code):
        return ResponseCode.RECOVERABLE <= code < ResponseCode.ERROR

    def rethrow(self, result):
        # Use the SNAPResponse.code to route the exception
        #   by re-parsing into an exception of the target type.

        if result.code >= ResponseCode.ERROR:
            raise RuntimeError(result.message)
        if result.code >= ResponseCode.RECOVERABLE:
            raise RecoverableException.parse_raw(result.message)
        if result.code == ResponseCode.LIVE_DATA_STATE:
            raise LiveDataState.parse_raw(result.message)
        if result.code == ResponseCode.USER_CANCELLATION:
            raise UserCancellation.parse_raw(result.message)
        if result.code == ResponseCode.CONTINUE_WARNING:
            raise ContinueWarning.parse_raw(result.message)
        if result.message:
            self.signalWarning.emit(result.message)

    def _handleComplications(self, code, message):
        if self._isErrorCode(code):
            QMessageBox.critical(
                self,
                "Error",
                f"Error {code}: {message}",
                QMessageBox.Ok,
                QMessageBox.Ok,
            )
        elif self._isRecoverableError(code):
            recoverableException = RecoverableException.parse_raw(message)
            if recoverableException.flags == RecoverableException.Type.STATE_UNINITIALIZED:
                self.handleStateMessage(recoverableException)
        elif code == ResponseCode.LIVE_DATA_STATE:
            liveDataInfo = LiveDataState.Model.model_validate_json(message)
            self.liveDataStateTransition.emit(liveDataInfo)
        elif code == ResponseCode.USER_CANCELLATION:
            userCancellationInfo = UserCancellation.Model.model_validate_json(message)
            self.userCancellation.emit(userCancellationInfo)
        elif code == ResponseCode.CONTINUE_WARNING:
            continueInfo = ContinueWarning.Model.model_validate_json(message)
            result = self._handleContinueWarning(continueInfo)
            if result != "&No":
                self.continueAnyway.emit(continueInfo)
        elif message:
            self._handleWarning(message)

    @Slot(str)
    def _handleWarning(self, message: str):
        messageBox = QMessageBox(
            QMessageBox.Warning, "Warning", "The backend has encountered warning(s)", QMessageBox.Ok, parent=self
        )
        messageBox.setDetailedText(f"{message}")
        messageBox.exec()

    def _handleContinueWarning(self, continueInfo: ContinueWarning.Model):
        if logger.isEnabledFor(logging.DEBUG):
            import traceback

            # print stacktrace
            logger.debug(f"`_handleContinueWarning`: `continueInfo`: {continueInfo}")
            traceback.print_stack()

        continueAnyway = QMessageBox(
            QMessageBox.Warning,
            "Warning",
            f"{continueInfo.message}",
            buttons=QMessageBox.Yes | QMessageBox.No,
            parent=self,
        )
        continueAnyway.setDefaultButton(QMessageBox.No)
        if continueInfo.flags == ContinueWarning.Type.MISSING_NORMALIZATION:
            continueAnyway.addButton("Continue without Normalization", QMessageBox.YesRole)
        continueAnyway.exec()
        clickedButton = continueAnyway.clickedButton().text()
        if clickedButton == "Continue without Normalization":
            continueInfo.flags |= ContinueWarning.Type.CONTINUE_WITHOUT_NORMALIZATION
        return continueAnyway.clickedButton().text()

    def handleStateMessage(self, recoverableException):
        """
        Handles a specific 'state' message.
        """
        recoveryData = recoverableException.data
        runNumber = recoveryData.get("runNumber")
        useLiteMode = recoveryData.get("useLiteMode")
        try:
            logger.info("Handling 'state' message.")
            initializationMenu = InitializationMenu(runNumber=runNumber, parent=self, useLiteMode=useLiteMode)
            initializationMenu.finished.connect(lambda: initializationMenu.deleteLater())
            initializationMenu.finished.connect(self.resetWorkflow)
            initializationMenu.show()
        except Exception as e:  # noqa: BLE001
            logger.warning(f"The 'state' handling method encountered an error:{str(e)}")
