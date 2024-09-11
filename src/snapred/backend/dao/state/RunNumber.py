from pydantic import BaseModel, Field, validator

from snapred.backend.log.logger import snapredLogger
from snapred.meta.validator.RunNumberValidator import RunNumberValidator

logger = snapredLogger.getLogger(__name__)


class RunNumber(BaseModel):
    runNumber: int = Field(..., description="The run number provided by the user for reduction.")

    @validator("runNumber", pre=True, always=True)
    def validateRunNumber(cls, value):
        # Check if run number is an integer
        if not isinstance(value, int):
            logger.error("Run number must be an integer.")
            raise ValueError("Run number must be an integer. Please enter a valid run number.")

        # Use RunNumberValidator to check the minimum run number and cached data existence
        if not RunNumberValidator.validateRunNumber(str(value)):
            logger.error("Run number does not meet the minimum required value or does not have cached data available.")
            raise ValueError(
                "Run number is below the minimum value or data does not exist. Please enter a valid run number."
            )

        return value
