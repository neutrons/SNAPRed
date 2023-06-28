from snapred.backend.log.logger import snapredLogger

logger = snapredLogger.getLogger(__name__)


class AlgorithmException(Exception):
    "Raised when a Mantid Algorithm fails"

    def __init__(self, algorithm, context=""):
        self.message = "Mantid Algorithm --  {}  -- has failed".format(algorithm)
        self.message += " -- " + context
        logger.error(self.message)
        super().__init__(self.message)
