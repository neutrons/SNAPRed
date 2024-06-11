import json
from typing import Any, List

import pydantic
from pydantic import BaseModel


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
    return pydantic.TypeAdapter(type_).validate_json(src)


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
