from typing import Any, Callable

from pydantic import BaseModel, field_serializer
from pydantic.json_schema import SkipJsonSchema


class Hook(BaseModel, arbitrary_types_allowed=True):
    func: SkipJsonSchema[Callable | str]
    kwargs: dict[str, Any] = {}

    def __init__(self, func: Callable, **kwargs) -> None:
        super().__init__(func=func, kwargs=kwargs)

    # convert func to method name when serialized with json
    @field_serializer("func")
    def serialize_func(self, value: Callable | str) -> str:
        """
        Serializes the function to its method name.
        """
        if isinstance(value, str):
            return value
        return value.__qualname__ if hasattr(value, "__qualname__") else str(value)
