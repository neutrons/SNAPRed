from typing import Any

from mantid.kernel import IntArrayMandatoryValidator, IntArrayProperty
from pydantic import (
    BaseModel,
    Field,
    GetCoreSchemaHandler,
    GetJsonSchemaHandler,
    ValidationError,
    validator,
)
from pydantic.json_schema import JsonSchemaValue
from pydantic_core import core_schema
from typing_extensions import Annotated

from snapred.backend.log.logger import snapredLogger
from snapred.meta.validator.RunNumberValidator import RunNumberValidator

logger = snapredLogger.getLogger(__name__)


class RunNumberImpl(BaseModel):
    runNumber: int = Field(..., description="The run number provided by the user for reduction.")

    def __str__(self):
        return str(self.runNumber)

    def toString(self) -> str:
        # return an actual string, not a `WorkspaceName`
        return super(self.__class__, self).__str__()

    @validator("runNumber", pre=True)
    def parse(cls, v):
        if isinstance(v, str):
            return RunNumber(runNumber=v)
        return v

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, RunNumberImpl):
            return self.runNumber == other.runNumber
        elif isinstance(other, int):
            return self.runNumber == other
        elif isinstance(other, str):
            return self.runNumber == int(other)
        return False

    @validator("runNumber", pre=True, always=True)
    def validateRunNumber(cls, value):
        # Check if run number is an
        if isinstance(value, str):
            try:
                value = int(value)
            except ValueError:
                logger.error(f"Run number {value} must be an integer.")
                raise ValueError(f"Run number {value} must be an integer. Please enter a valid run number.")
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


class _ThirdPartyTypePydanticAnnotation:
    @classmethod
    def __get_pydantic_core_schema__(
        cls,
        _source_type: Any,
        _handler: GetCoreSchemaHandler,
    ) -> core_schema.CoreSchema:
        """
        We return a pydantic_core.CoreSchema that behaves in the following ways:

        * ints will be parsed as `ThirdPartyType` instances with the int as the x attribute
        * `ThirdPartyType` instances will be parsed as `ThirdPartyType` instances without any changes
        * Nothing else will pass validation
        * Serialization will always return just an int
        """

        def validate_from_str(value: str) -> RunNumberImpl:
            result = RunNumberImpl(runNumber=value)
            return result

        from_str_schema = core_schema.chain_schema(
            [
                core_schema.str_schema(),
                core_schema.no_info_plain_validator_function(validate_from_str),
            ]
        )

        return core_schema.json_or_python_schema(
            json_schema=from_str_schema,
            python_schema=core_schema.union_schema(
                [
                    # check if it's an instance first before doing any further work
                    core_schema.is_instance_schema(RunNumberImpl),
                    from_str_schema,
                ]
            ),
            serialization=core_schema.plain_serializer_function_ser_schema(lambda instance: instance.x),
        )

    @classmethod
    def __get_pydantic_json_schema__(
        cls, _core_schema: core_schema.CoreSchema, handler: GetJsonSchemaHandler
    ) -> JsonSchemaValue:
        # Use the same schema that would be used for `str`
        return handler(core_schema.str_schema())


RunNumber = Annotated[RunNumberImpl, _ThirdPartyTypePydanticAnnotation]
