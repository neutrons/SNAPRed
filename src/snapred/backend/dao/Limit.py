from typing import Generic, TypeVar

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
            value, minV, maxV = (values["value"], values["minimum"], values["maximum"])
            raise ValueError(f"value {value} must be between min {minV} and max {maxV}")
        return values

    # TODO for use in new pydantic
    # @model_validator(mode='before')
    # def validate_scalar_fields(cls, values):
    #     assert values.get("minimum") <= values.get("value") <= values.get("maximum")
    #     return values
