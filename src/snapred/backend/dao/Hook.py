from typing import Any, Callable

from pydantic import BaseModel, field_serializer
from pydantic.json_schema import SkipJsonSchema


class Hook(BaseModel, arbitrary_types_allowed=True):
    func: SkipJsonSchema[Callable]
    kwargs: dict[str, Any] = {}

    def __init__(self, func: Callable, **kwargs) -> None:
        super().__init__(func=func, kwargs=kwargs)

    # convert func to method name when serialized with json
    @field_serializer("func")
    def serialize_func(self, value: Callable) -> str:
        """
        Serializes the function to its method name.
        """
        return value.__name__ if hasattr(value, "__name__") else str(value)
