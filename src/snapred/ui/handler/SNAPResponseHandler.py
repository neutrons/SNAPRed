from qtpy.QtCore import QObject, Signal
from qtpy.QtWidgets import QMessageBox, QWidget

from snapred.backend.dao.SNAPResponse import ResponseCode
from snapred.backend.log.logger import snapredLogger

logger = snapredLogger.getLogger(__name__)


class SNAPResponseHandler(QObject):
    signal = Signal(int, str, object)

    def __init__(self):
        super().__init__(None)

        self.signal.connect(self._handleComplications)

    def _isErrorCode(self, code):
        return code >= ResponseCode.ERROR

    def _isRecoverableError(self, code):
        return ResponseCode.RECOVERABLE <= code < ResponseCode.ERROR

    def handle(self, result, view):
        self.signal.emit(result.code, result.message, view)

    def _handleComplications(self, code, message, view):
        if self._isErrorCode(code):
            QMessageBox.critical(
                view,
                "Error",
                f"Error {code}: {message}",
                QMessageBox.Ok,
                QMessageBox.Ok,
            )
        elif self._isRecoverableError(code):
            if "state" in message:
                self.handleStateMessage(view)
            else:
                logger.error(f"Unhandled scenario triggered by state message: {message}")
                messageBox = QMessageBox(
                    QMessageBox.Warning,
                    "Warning",
                    "Proccess completed successfully with warnings!",
                    QMessageBox.Ok,
                    view,
                )
                messageBox.setDetailedText(f"{message}")
                messageBox.exec()
        elif message:
            messageBox = QMessageBox(
                QMessageBox.Warning,
                "Warning",
                "Proccess completed successfully with warnings!",
                QMessageBox.Ok,
                view,
            )
            messageBox.setDetailedText(f"{message}")
            messageBox.exec()

    def handleStateMessage(self, view):
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
