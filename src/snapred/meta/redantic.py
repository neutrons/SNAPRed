import ctypes
import json
from pathlib import Path
from typing import Any, List, Type, TypeVar, Union

from pydantic import BaseModel, TypeAdapter

T = TypeVar("T")


def parse_obj_as(type_: Type[T], obj: Any) -> T:
    if isinstance(obj, BaseModel):
        obj = obj.model_dump()
    return TypeAdapter(type_).validate_python(obj)


def parse_raw_as(type_: Type[T], b: Union[str, bytes]) -> T:
    return TypeAdapter(type_).validate_json(b)


def parse_file_as(type_: Type[T], path: Union[str, Path]) -> T:
    with open(path, "r") as f:
        obj = parse_raw_as(type_, f.read())
    return obj


def list_to_raw(baseModelList: List[BaseModel]):
    return json.dumps([baseModel.dict() for baseModel in baseModelList])


def list_to_raw_pretty(baseModelList: List[BaseModel]):
    return json.dumps([baseModel.dict() for baseModel in baseModelList], indent=4)


def list_from_raw(type_: Any, src: str) -> List[BaseModel]:
    # parse a `List[BaseModel]` from a json string
    if (
        not (hasattr(type_, "__origin__") and hasattr(type_, "__args__"))
        or not issubclass(type_.__origin__, list)
        or not issubclass(type_.__args__[0], BaseModel)
    ):
        raise TypeError(f"target type must derive from 'List[BaseModel]' not {type_}")
    return TypeAdapter(type_).validate_json(src)


def access_pointer(pointer: int) -> Any:
    return ctypes.cast(pointer, ctypes.py_object).value


def write_model(baseModel: BaseModel, path):
    with open(path, "w") as f:
        # Note: 'json.dumps(baseModel)' adds spaces after separators,
        #   'baseModel.model_dump_json(indent=None)' does not
        f.write(json.dumps(baseModel.dict()))


def write_model_pretty(baseModel: BaseModel, path):
    with open(path, "w") as f:
        f.write(baseModel.model_dump_json(indent=4))


def write_model_list(baseModelList: List[BaseModel], path):
    with open(path, "w") as f:
        f.write(list_to_raw(baseModelList))


def write_model_list_pretty(baseModelList: List[BaseModel], path):
    with open(path, "w") as f:
        f.write(list_to_raw_pretty(baseModelList))
