from snapred.backend.log.logger import snapredLogger

logger = snapredLogger.getLogger(__name__)


class UserException(Exception):
    "Raised when a User supplies invalid input"

    def __init__(self, message):
        self.message = "User Error -- {}".format(message)
        logger.error(self.message)
        super().__init__(self.message)
