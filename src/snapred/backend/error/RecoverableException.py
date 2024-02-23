from typing import Any, Literal

from snapred.backend.log.logger import snapredLogger

logger = snapredLogger.getLogger(__name__)

RecoverableErrorType = Literal["State not initialized", "Another recoverable condition"]


class RecoverableException(Exception):
    """
    Raised when a recoverable exception is thrown.
    Allows for custom error types and optional extra context.
    """

    def __init__(self, exception: Exception, errorType: RecoverableErrorType, **kwargs: Any):
        self.errorType = errorType
        self.message = f"A recoverable error occurred: {self.errorType}"
        self.extraContext = kwargs
        self.stateMessageHandled = False

        logMessage = f"{self.message} Original exception: {str(exception)}"

        if self.extraContext:
            logMessage += f" | Context: {self.extraContext}"

        logger.error(logMessage)
        super().__init__(self.message)

    def handleStateMessage(self):
        """
        Handles a specific 'state' message.
        """
        if self.errorType == "State not initialized":
            logger.info("Handling 'state' message.")
            self.stateMessageHandled = True
        else:
            logger.warning(
                "The 'handleStateMessage' method was called, but the error type is not related to state initialization."
            )
