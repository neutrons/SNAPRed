from pydantic import BaseModel
from snapred.backend.log.logger import snapredLogger
from snapred.meta.Config import Config

logger = snapredLogger.getLogger(__name__)


class RunNumberValidator(BaseModel):
    @classmethod
    def validateRunNumber(cls, runNumber: str):
        if not runNumber.isdigit():
            logger.warning("Run number must be a positive integer")
            return False
        number = int(runNumber)
        minRunNumber = Config["instrument.startingRunNumber"]
        maxRunNumber = Config["instrument.maximumRunNumber"]
        if number < minRunNumber:
            logger.warning("Run number is below minimum value of " + str(minRunNumber))
            return False
        if number > maxRunNumber:
            logger.warning("Run number exceeds max value of " + str(maxRunNumber))
            return False
        return True
