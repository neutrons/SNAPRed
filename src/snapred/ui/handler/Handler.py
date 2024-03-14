from qtpy.QtWidgets import QMessageBox

from snapred.backend.dao.SNAPResponse import ResponseCode
from snapred.backend.log.logger import snapredLogger

logger = snapredLogger.getLogger(__name__)


class Hanlder(object):
    def __init__(self, result, view):
        self._handleComplications(result, view)

    def _isErrorCode(self, code):
        return code >= ResponseCode.ERROR

    def _isRecoverableError(self, code):
        return ResponseCode.RECOVERABLE <= code < ResponseCode.ERROR

    def _handleComplications(self, result, view):
        if self._isErrorCode(result.code):
            QMessageBox.critical(
                view,
                "Error",
                f"Error {result.code}: {result.message}",
                QMessageBox.Ok,
                QMessageBox.Ok,
            )
        elif self._isRecoverableError(result.code):
            if "state" in result.message:
                self.handleStateMessage(self.view)
            else:
                logger.error(f"Unhandled scenario triggered by state message: {result.message}")
                messageBox = QMessageBox(
                    QMessageBox.Warning,
                    "Warning",
                    "Proccess completed successfully with warnings!",
                    QMessageBox.Ok,
                    view,
                )
                messageBox.setDetailedText(f"{result.message}")
                messageBox.exec()
        elif result.message:
            messageBox = QMessageBox(
                QMessageBox.Warning,
                "Warning",
                "Proccess completed successfully with warnings!",
                QMessageBox.Ok,
                view,
            )
            messageBox.setDetailedText(f"{result.message}")
            messageBox.exec()
