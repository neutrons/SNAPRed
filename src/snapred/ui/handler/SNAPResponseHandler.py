import threading

from qtpy.QtCore import Signal, Slot
from qtpy.QtWidgets import QMessageBox, QWidget

from snapred.backend.dao.SNAPResponse import ResponseCode, SNAPResponse
from snapred.backend.error.ContinueWarning import ContinueWarning
from snapred.backend.error.UserCancellation import UserCancellation
from snapred.backend.error.LiveDataState import LiveDataState
from snapred.backend.error.RecoverableException import RecoverableException
from snapred.backend.log.logger import snapredLogger
from snapred.ui.view.InitializeStateCheckView import InitializationMenu

logger = snapredLogger.getLogger(__name__)


class SNAPResponseHandler(QWidget):
    signal = Signal(object)
    signalWarning = Signal(str, object)
    continueAnyway = Signal(object)
    userCancellation = Signal(object)
    liveDataStateTransition = Signal(object)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.signal.connect(self._handle)
        self.signalWarning.connect(SNAPResponseHandler._handleWarning)

    def handle(self, result):
        self.signal.emit(result)

    @Slot(SNAPResponse)
    def _handle(self, result):
        # if no complications, do nothing here (program will continue)
        # if errors, do nothing here (program will halt)
        # if a continue warning was raised, receive what user selected
        # if the user selected to continue anyway, then emit the signal to continue anyway
        
        if threading.current_thread() is threading.main_thread():  
            SNAPResponseHandler._handleComplications(result.code, result.message, self)
        else:
            # As a slot of the `SNAPResponseHandler` this method should only have been
            #   executed on the main thread.
            logger.error(
                "IMPLEMENTATION ERROR: this should never happen!"
                + f"\n SNAPResponseHandler.rethrow: {result}"
            )
            self.rethrow(result)

    @staticmethod
    def _isErrorCode(code):
        return code >= ResponseCode.ERROR

    @staticmethod
    def _isRecoverableError(code):
        return ResponseCode.RECOVERABLE <= code < ResponseCode.ERROR

    def rethrow(self, result):
        # TODO: A reparse and rethrow by itself will not change the thread of execution.
        #   Was this method intended to be used as a slot?
        
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
            self.signalWarning.emit(result.message, self)

    # TODO: view is only ever handler, maybe this variable should be updated to reflect that?
    @staticmethod
    def _handleComplications(code, message, view):
        if SNAPResponseHandler._isErrorCode(code):
            QMessageBox.critical(
                view,
                "Error",
                f"Error {code}: {message}",
                QMessageBox.Ok,
                QMessageBox.Ok,
            )
        elif SNAPResponseHandler._isRecoverableError(code):
            recoverableException = RecoverableException.parse_raw(message)
            if recoverableException.flags == RecoverableException.Type.STATE_UNINITIALIZED:
                SNAPResponseHandler.handleStateMessage(view, recoverableException)
        elif code == ResponseCode.LIVE_DATA_STATE:
            liveDataInfo = LiveDataState.Model.model_validate_json(message)
            view.liveDataStateTransition.emit(liveDataInfo)
        elif code == ResponseCode.USER_CANCELLATION:
            userCancellationInfo = UserCancellation.Model.model_validate_json(message)
            view.userCancellation.emit(userCancellationInfo)        
        elif code == ResponseCode.CONTINUE_WARNING:
            continueInfo = ContinueWarning.Model.model_validate_json(message)
            if SNAPResponseHandler._handleContinueWarning(continueInfo, view):
                view.continueAnyway.emit(continueInfo)
        elif message:
            SNAPResponseHandler._handleWarning(message, view)

    @staticmethod
    def _handleWarning(message, view):
        messageBox = QMessageBox(
            QMessageBox.Warning,
            "Warning",
            "The backend has encountered warning(s)",
            QMessageBox.Ok,
            view,
        )
        messageBox.setDetailedText(f"{message}")
        messageBox.exec()

    @staticmethod
    def _handleContinueWarning(continueInfo: ContinueWarning.Model, view):
        # print stacktrace
        logger.info("It happens here and here")
        import traceback

        traceback.print_stack()

        continueAnyway = QMessageBox.warning(
            view,
            "Warning",
            continueInfo.message,
            buttons=QMessageBox.Yes | QMessageBox.No,
            defaultButton=QMessageBox.No,
        )
        return continueAnyway == QMessageBox.Yes

    @staticmethod
    def handleStateMessage(view, recoverableException):
        """
        Handles a specific 'state' message.
        """
        recoveryData = recoverableException.data
        runNumber = recoveryData.get("runNumber")
        useLiteMode = recoveryData.get("useLiteMode")
        try:
            logger.info("Handling 'state' message.")
            initializationMenu = InitializationMenu(runNumber=runNumber, parent=view, useLiteMode=useLiteMode)
            initializationMenu.finished.connect(lambda: initializationMenu.deleteLater())
            initializationMenu.show()
        except Exception as e:  # noqa: BLE001
            logger.warning(f"The 'state' handling method encountered an error:{str(e)}")
