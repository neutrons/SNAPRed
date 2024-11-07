from snapred.backend.log.logger import snapredLogger
from snapred.meta.Config import Config

logger = snapredLogger.getLogger(__name__)


class RunNumberValidator:
    @classmethod
    def validateRunNumber(cls, runNumber: str) -> bool:
        if not runNumber.isdigit():
            logger.warning(f"Run number {runNumber} must be a positive integer")
            return False
        number = int(runNumber)
        minRunNumber = Config["instrument.minimumRunNumber"]
        if number < minRunNumber:
            logger.warning(f"Run number {runNumber} is below minimum value of {minRunNumber}")
            return False
        return True
