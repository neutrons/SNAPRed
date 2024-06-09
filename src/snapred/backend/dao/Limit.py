from enum import IntEnum
from typing import Generic, Tuple, TypeVar

from pydantic import BaseModel, model_validator

T = TypeVar("T")


class Limit(BaseModel, Generic[T]):
    """
    The Limit class defines minimum and maximum limits for a given
    type `T`, ensuring through validation that the minimum is not
    greater than the maximum. This class is generic, allowing for
    the application of limits to various data types.
    """

    minimum: T
    maximum: T

    @model_validator(mode="after")
    def validate_scalar_fields(self):
        if self.minimum > self.maximum:
            raise ValueError("min cannot be greater than max")
        return self


class Pair(BaseModel, Generic[T]):
    """
    The `Pair` class primarily facilitates the mapping of two related values,
    adaptable to contexts where these values need to be interpreted or
    manipulated as a unit. This is also a generic class.
    """

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


class LimitedValue(BaseModel, Generic[T]):
    """
    `LimitedValue` ensures that a given value of type `T` falls within specified
    minimum and maximum limits. It enforces the constraint that the value must
    always be between these defined limits.
    """

    value: T
    minimum: T
    maximum: T

    @model_validator(mode="after")
    def validate_scalar_fields(self):
        if not self.minimum <= self.value <= self.maximum:
            raise ValueError(f"value {self.value} must be between min {self.minimum} and max {self.maximum}")
        return self


class BinnedValue(BaseModel, Generic[T]):
    """
    `BinnedValue` defines a range and bin width for values of type `T`,
    alongside a binning mode (linear or logarithmic). It validates the
    range and bin width, adjusting bin width to positive if negative,
    ensuring logical binning behavior for data categorization or
    histogram generation.
    """

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

    @model_validator(mode="after")
    def validate_scalar_fields(self):
        if not self.minimum < self.maximum:
            raise ValueError(f"min {self.minimum} cannot be greater than max {self.maximum}")
        if self.binWidth < 0:
            self.binWidth = abs(self.binWidth)
        return self
