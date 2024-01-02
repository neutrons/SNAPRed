from snapred.backend.log.logger import snapredLogger

logger = snapredLogger.getLogger(__name__)


class StateValidationException(Exception):
    "Raised when an Instrument State is invalid"

    def __init__(self, exception: Exception):
        self.message = "Instrument State for given Run Number is invalid! (see logs for details.)"
        logger.error(str(exception))
        super().__init__(self.message)
