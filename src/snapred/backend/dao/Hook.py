from types import FunctionType
from typing import Annotated, Any, ClassVar, Dict, FrozenSet, List, get_args, get_origin

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


def typeName(cls: type) -> str:
    """The name under which a type is recorded in `Hook.kwargTypes`: e.g. "builtins.tuple"."""
    if get_origin(cls) is Annotated:
        # e.g. `WorkspaceName`, which is exported as an `Annotated` alias of its class
        cls = get_args(cls)[0]
    return f"{cls.__module__}.{cls.__qualname__}"


def _pointerToken(key: Any) -> str:
    # RFC 6901 JSON pointer escaping
    return str(key).replace("~", "~0").replace("/", "~1")


class Hook(BaseModel, arbitrary_types_allowed=True):
    """
    A function, with its keyword arguments, to be called at a named point of a recipe.

    The arguments' types are recorded in `kwargTypes` when the hook is created, because serialization keeps only
    their values: e.g. a `WorkspaceName` argument is serialized as a plain string.  `kwargTypes` maps the JSON pointer
    of each argument value (e.g. "/masks/0") whose type JSON can't represent to that type's `typeName`.
    `kwargTypes` is serialized with the hook, so the types remain known for a hook that was read back from a record.
    """

    # argument names that would be ambiguous with the serialized form of a hook
    RESERVED_ARGS: ClassVar[FrozenSet[str]] = frozenset({"kwargs", "kwargTypes"})
    # types that JSON represents exactly: these are not recorded in `kwargTypes`
    _JSON_TYPES: ClassVar[tuple] = (str, int, float, bool, type(None), list, dict)

    p_func: SkipJsonSchema[FunctionType | str] = Field(
        ...,
        alias="func",
    )
    kwargs: Dict[str, Any] = {}
    kwargTypes: Dict[str, str] = {}

    def __init__(self, func: FunctionType | str, **kwargs) -> None:
        if "kwargs" in kwargs and set(kwargs) <= self.RESERVED_ARGS:
            # the serialized form of a hook: e.g. a hook read back from a record
            super().__init__(func=func, **kwargs)
            return
        reserved = self.RESERVED_ARGS & set(kwargs)
        if reserved:
            raise ValueError(f"Hook arguments cannot be named {sorted(reserved)}")
        super().__init__(func=func, kwargs=kwargs, kwargTypes=self.argumentTypes(kwargs))

    @classmethod
    def argumentTypes(cls, kwargs: Dict[str, Any]) -> Dict[str, str]:
        """The JSON pointer of each argument value whose type JSON can't represent, mapped to that type's name."""
        types: Dict[str, str] = {}

        def visit(pointer: str, value: Any):
            if type(value) not in cls._JSON_TYPES:
                types[pointer] = typeName(type(value))
            if isinstance(value, dict):
                for key, item in value.items():
                    visit(f"{pointer}/{_pointerToken(key)}", item)
            elif isinstance(value, (list, tuple)):
                for index, item in enumerate(value):
                    visit(f"{pointer}/{index}", item)

        for name, value in kwargs.items():
            visit(f"/{_pointerToken(name)}", value)
        return types

    def argumentsOfType(self, cls: type) -> Dict[str, Any]:
        """The argument values recorded as being of type `cls`, by JSON pointer."""
        name = typeName(cls)
        return {pointer: self._resolve(pointer) for pointer, recorded in self.kwargTypes.items() if recorded == name}

    def _resolve(self, pointer: str) -> Any:
        value: Any = self.kwargs
        for token in pointer.split("/")[1:]:
            key = token.replace("~1", "/").replace("~0", "~")
            value = value[int(key)] if isinstance(value, (list, tuple)) else value[key]
        return value

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


def hookArgumentsOfType(hooks: Dict[str, List[Hook]] | None, cls: type) -> List[Any]:
    """Every argument value of type `cls`, over all of `hooks`, in order."""
    if not hooks:
        return []
    return [value for hookList in hooks.values() for hook in hookList for value in hook.argumentsOfType(cls).values()]
