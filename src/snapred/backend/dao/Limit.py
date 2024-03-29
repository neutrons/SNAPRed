from enum import IntEnum
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


class Pair(GenericModel, Generic[T]):
    left: T
    right: T

    def __init__(self, minimum: T = None, maximum: T = None, **kwargs):
        if minimum is not None and maximum is not None:
            super().__init__(left=minimum, right=maximum)
        else:
            super().__init__(**kwargs)

    @property
    def minimum(self) -> T:
        return self.left

    @property
    def maximum(self) -> T:
        return self.right


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


class BinnedValue(GenericModel, Generic[T]):
    minimum: T
    binWidth: T
    maximum: T

    class BinningMode(IntEnum):
        LOG = -1
        LINEAR = 1

    binningMode: BinningMode = BinningMode.LOG

    @property
    def params(self) -> Tuple[float, float, float]:
        return (self.minimum, self.binningMode * abs(self.binWidth), self.maximum)

    @root_validator(allow_reuse=True)
    def validate_scalar_fields(cls, values):
        if not values.get("minimum") < values.get("maximum"):
            raise ValueError("min cannot be greater than max")
        if values.get("binWidth") < 0:
            values["binWidth"] = abs(values.get("binWidth"))
        return values
