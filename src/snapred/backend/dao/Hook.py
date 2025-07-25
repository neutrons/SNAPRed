from types import FunctionType
from typing import Any

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    SerializationInfo,
    SerializerFunctionWrapHandler,
    field_serializer,
    model_serializer,
)
from pydantic.json_schema import SkipJsonSchema


class Hook(BaseModel, arbitrary_types_allowed=True):
    p_func: SkipJsonSchema[FunctionType | str] = Field(
        ...,
        alias="func",
    )
    kwargs: dict[str, Any] = {}

    def __init__(self, func: FunctionType, **kwargs) -> None:
        super().__init__(func=func, kwargs=kwargs)

    # func getter that throws an error if func is string
    @property
    def func(self) -> FunctionType:
        if isinstance(self.p_func, str):
            raise TypeError("This Hook instance is just a record, func no longer available.")
        return self.p_func

    @property
    def funcStr(self) -> str:
        """
        Returns the function as a string.
        """
        return self.p_func if isinstance(self.p_func, str) else self.p_func.__qualname__

    # convert func to method name when serialized with json
    @field_serializer("p_func")
    def serialize_func(self, value: FunctionType | str) -> str:
        """
        Serializes the function to its qualified name.
        """
        if isinstance(value, str):
            return value
        return value.__qualname__ if hasattr(value, "__qualname__") else str(value)

    @model_serializer(when_used="always", mode="wrap")
    def serialize(
        self,
        serializer: SerializerFunctionWrapHandler,
        info: SerializationInfo,  # noqa: ARG002
    ) -> dict[str, Any]:
        data = serializer(self)
        data["func"] = data.pop("p_func")
        return data

    model_config = ConfigDict(arbitrary_types_allowed=True)
