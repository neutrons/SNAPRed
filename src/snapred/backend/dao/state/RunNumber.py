from mantid.kernel import IntArrayMandatoryValidator, IntArrayProperty
from pydantic import BaseModel, Field, ValidationError, field_validator

from snapred.backend.log.logger import snapredLogger
from snapred.meta.validator.RunNumberValidator import RunNumberValidator

logger = snapredLogger.getLogger(__name__)


class RunNumber(BaseModel):
    runNumber: int = Field(..., description="The run number provided by the user for reduction.")

    @field_validator("runNumber", mode="after")
    @classmethod
    def validateRunNumber(cls, value):
        # Check if run number is an integer
        if not isinstance(value, int):
            logger.error(f"Run number {value} must be an integer.")
            raise ValueError(f"Run number {value} must be an integer. Please enter a valid run number.")

        # Use RunNumberValidator to check the minimum run number and cached data existence
        if not RunNumberValidator.validateRunNumber(str(value)):
            logger.error(
                f"Run number {value} does not meet the minimum required value or does not have cached data available."
            )
            raise ValueError(
                f"Run number {value} is below the minimum value or data does not exist."
                "Please enter a valid run number."
            )

        return value

    @classmethod
    def runsFromIntArrayProperty(cls, runs: str, throws=True):
        # This will toss an error if supplied garbage input to IntArrayProperty
        iap = IntArrayProperty(
            name="RunList",
            values=runs,
            validator=IntArrayMandatoryValidator(),
        )
        prospectiveRuns = iap.value
        validRuns = []
        # TODO: When RunNumber is actually supported this should return a list of RunNumber objects and not Str
        errors = []
        for run in prospectiveRuns:
            try:
                validRuns.append(str(RunNumber(runNumber=int(run)).runNumber))
            except ValidationError as e:
                err = e.errors()[0]["msg"]
                errors.append((run, err))

        if throws and errors:
            raise ValueError(f"Errors in run numbers: {errors}")

        return validRuns, errors
