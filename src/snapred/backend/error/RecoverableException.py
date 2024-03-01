from typing import Any, Literal

from snapred.backend.log.logger import snapredLogger

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

    def __init__(self, exception: Exception, errorMsg: RecoverableErrorType, **kwargs: Any):
        self.message = errorMsg
        self.extraContext = kwargs

        logMessage = f"{self.message} Original exception: {str(exception)}"

        if self.extraContext:
            logMessage += f" | Context: {self.extraContext}"
        logger.error(logMessage)
        super().__init__(self.message)
