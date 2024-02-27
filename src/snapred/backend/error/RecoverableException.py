from typing import Any, Literal

from snapred.backend.log.logger import snapredLogger
from snapred.ui.view.InitializeCalibrationCheckView import CalibrationMenu

logger = snapredLogger.getLogger(__name__)

RecoverableErrorType = Literal[
    "AttributeError: 'NoneType' object has no attribute 'instrumentState'",
    "etc",
]


class RecoverableException(Exception):
    """
    Raised when a recoverable exception is thrown.
    Allows for custom error types and optional extra context.
    """

    def __init__(self, exception: Exception, errorType: RecoverableErrorType, **kwargs: Any):
        self.errorType = errorType
        if errorType == "AttributeError: 'NoneType' object has no attribute 'instrumentState'":
            self.message = f"A recoverable error occurred: {self.errorType}"
        else:
            self.message = "A unspecified recoverable error occurred."
        self.extraContext = kwargs

        logMessage = f"{self.message} Original exception: {str(exception)}"
        if self.extraContext:
            logMessage += f" | Context: {self.extraContext}"
        logger.error(logMessage)
        super().__init__(self.message)

    def handleStateMessage(self, runNumber, view):
        """
        Handles a specific 'state' message.
        """
        if self.errorType == "AttributeError: 'NoneType' object has no attribute 'instrumentState'":
            logger.info("Handling 'state' message.")
            calibrationMenu = CalibrationMenu(runNumber=runNumber, parent=view)
            calibrationMenu.exec_()
        else:
            logger.warning(
                "The 'handleStateMessage' method was called, but the error type is not related to state initialization."
            )
