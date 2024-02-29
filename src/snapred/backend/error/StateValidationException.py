import re

from snapred.backend.log.logger import snapredLogger

logger = snapredLogger.getLogger(__name__)


class StateValidationException(Exception):
    "Raised when an Instrument State is invalid"

    def __init__(self, exception: Exception):
        exceptionStr = str(exception)

        path_pattern = r"(/[^'\"]+)"
        match = re.search(path_pattern, exceptionStr)

        if "/SNS/SNAP/" in exceptionStr and match:
            path = match.group(1)
            self.message = f"You don't have permission to write to analysis directory: {path}."
        else:
            self.message = "Instrument State for given Run Number is invalid! (see logs for details.)"

        logger.error(exceptionStr)
        super().__init__(self.message)
