import functools
import inspect
import json
from typing import Any, Dict, List

from pydantic import BaseModel


def isListOfBaseModel(annotation: type) -> bool:
    # check if list and is a subclass of BaseModel
    return getattr(annotation, "__origin__", None) is list and issubclass(annotation.__args__[0], BaseModel)


def isBaseModel(clazz: type) -> bool:
    return inspect.isclass(clazz) and issubclass(clazz, BaseModel)


# only works on postional args for now
def FromString(func: callable):
    @functools.wraps(func)
    def inner(*args, **kwargs):
        """
        do operations with func
        """
        # get type of funct args
        fullArgSpec: tuple = inspect.getfullargspec(func)
        func_args: List = fullArgSpec.args
        func_annotations: Dict[str, Any] = fullArgSpec.annotations
        # map args to func_args
        argMap = dict(zip(func_args, args))
        # filter out self from argMap
        zelf = argMap.pop("self", None)

        # filter out args that are not BaseModel or List[BaseModel]
        baseModelArgs = {
            k: v
            for k, v in argMap.items()
            if isBaseModel(func_annotations[k]) or isListOfBaseModel(func_annotations[k])
        }
        # filter out arg values that are not strings
        stringArgs = {k: v for k, v in baseModelArgs.items() if isinstance(v, str)}

        # for each string arg, import the class and create an instance
        for k, v in stringArgs.items():
            if isBaseModel(func_annotations[k]):
                argMap[k] = func_annotations[k].parse_raw(v)
            elif isListOfBaseModel(func_annotations[k]):
                rawBaseModels = json.loads(v)
                # if not a list, make it a list
                if not isinstance(rawBaseModels, List):
                    rawBaseModels = [rawBaseModels]
                # load as list of base models
                argMap[k] = [func_annotations[k].__args__[0](**rawBaseModel) for rawBaseModel in rawBaseModels]

        argMap["self"] = zelf
        # replace args with new args in order
        args = (argMap[k] for k in func_args if k in argMap)

        return func(*args, **kwargs)

    return inner
    # issubclass(func_annotations['request'].__args__[0], BaseModel)
