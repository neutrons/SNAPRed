from typing import Generic, Literal, Tuple, TypeVar

from pydantic import root_validator
from pydantic.generics import GenericModel

T = TypeVar("T")


class Limit(GenericModel, Generic[T]):
    minimum: T
    maximum: T

    @root_validator(allow_reuse=True)
    def validate_scalar_fields(cls, values):
        if values.get("minimum") > values.get("maximum"):
            raise ValueError("min cannot be greater than max")
        return values

    # TODO for use in new pydantic
    # @model_validator(mode='before')
    # def validate_scalar_fields(cls, values):
    #     assert values.get("minimum") <= values.get("maximum")
    #     return values


class LimitedValue(GenericModel, Generic[T]):
    value: T
    minimum: T
    maximum: T

    @root_validator(allow_reuse=True)
    def validate_scalar_fields(cls, values):
        if not values.get("minimum") <= values.get("value") <= values.get("maximum"):
            raise ValueError("value has to be between min and max")
        return values

    # TODO for use in new pydantic
    # @model_validator(mode='before')
    # def validate_scalar_fields(cls, values):
    #     assert values.get("minimum") <= values.get("value") <= values.get("maximum")
    #     return values


class BinnedValue(GenericModel, Generic[T]):
    minimum: T
    binWidth: T
    maximum: T
    binMode: Literal["Linear", "Logarithmic"]

    @property
    def params(self) -> Tuple[float, float, float]:
        return (self.minimum, self.binWidth, self.maximum)

    @root_validator(allow_reuse=True)
    def validate_scalar_fields(cls, values):
        if not values.get("minimum") < values.get("maximum"):
            raise ValueError("min cannot be greater than max")
        if values.get("binWidth") < 0:
            values["binWidth"] = abs(values.get("binWidth"))
        return values
