from qtpy.QtCore import Signal
from qtpy.QtWidgets import QMessageBox, QWidget

from snapred.backend.dao.SNAPResponse import ResponseCode
from snapred.backend.error.RecoverableException import RecoverableException
from snapred.backend.log.logger import snapredLogger

logger = snapredLogger.getLogger(__name__)


class SNAPResponseHandler(QWidget):
    signal = Signal(object)
    signalWarning = Signal(str, object)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.signal.connect(self._handle)
        self.signalWarning.connect(SNAPResponseHandler._handleWarning)

    def handle(self, result):
        self.signal.emit(result)

    def _handle(self, result):
        SNAPResponseHandler._handleComplications(result.code, result.message, self)

    def _isErrorCode(code):
        return code >= ResponseCode.ERROR

    def _isRecoverableError(code):
        return ResponseCode.RECOVERABLE <= code < ResponseCode.ERROR

    def rethrow(self, result):
        if result.code >= ResponseCode.ERROR:
            raise RuntimeError(result.message)
        if result.code >= ResponseCode.RECOVERABLE:
            raise RecoverableException(result.message, "state")
        if result.message:
            self.signalWarning.emit(result.message, self)

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
            if "state" in message:
                SNAPResponseHandler.handleStateMessage(view)
            else:
                logger.error(f"Unhandled scenario triggered by state message: {message}")
                messageBox = QMessageBox(
                    QMessageBox.Warning,
                    "Warning",
                    "The backend completed a step with warning(s)",
                    QMessageBox.Ok,
                    view,
                )
                messageBox.setDetailedText(f"{message}")
                messageBox.exec()
        elif message:
            SNAPResponseHandler._handleWarning(message, view)

    @staticmethod
    def _handleWarning(message, view):
        messageBox = QMessageBox(
            QMessageBox.Warning,
            "Warning",
            "The backend completed a step with warning(s)",
            QMessageBox.Ok,
            view,
        )
        messageBox.setDetailedText(f"{message}")
        messageBox.exec()

    @staticmethod
    def handleStateMessage(view):
        """
        Handles a specific 'state' message.
        """
        from snapred.backend.dao.request.InitializeStateHandler import InitializeStateHandler
        from snapred.ui.view.InitializeStateCheckView import InitializationMenu

        try:
            logger.info("Handling 'state' message.")
            initializationMenu = InitializationMenu(runNumber=InitializeStateHandler.runId, parent=view)
            initializationMenu.finished.connect(lambda: initializationMenu.deleteLater())
            initializationMenu.show()
        except Exception as e:  # noqa: BLE001
            logger.warning(f"The 'state' handling method encountered an error:{str(e)}")
