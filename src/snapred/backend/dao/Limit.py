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


class LimitedValue(GenericModel, Generic[T]):
    value: T
    minimum: T
    maximum: T

    @root_validator(allow_reuse=True)
    def validate_scalar_fields(cls, values):
        if not values.get("minimum") <= values.get("value") <= values.get("maximum"):
            raise ValueError("value has to be between min and max")
        return values
