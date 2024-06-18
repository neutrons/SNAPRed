from snapred.backend.log.logger import snapredLogger
from snapred.meta.Config import Config

logger = snapredLogger.getLogger(__name__)


class RunNumberValidator:
    @classmethod
    def validateRunNumber(cls, runNumber: str) -> bool:
        if not runNumber.isdigit():
            logger.warning("Run number must be a positive integer")
            return False
        number = int(runNumber)
        minRunNumber = Config["instrument.minimumRunNumber"]
        if number <= minRunNumber:
            logger.warning("Run number is below minimum value of " + str(minRunNumber))
            return False
        return True
