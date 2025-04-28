import functools
import inspect
from typing import Any, Callable, List

from snapred.meta.Config import Config


class ConfigValue:
    def __init__(self, value: Any):
        self.value = value


def ConfigDefault(func: Callable[..., Any]):
    @functools.wraps(func)
    def inner(*args, **kwargs):
        fullArgSpec: tuple = inspect.getfullargspec(func)
        func_args: List = fullArgSpec.args
        # map args to func_args
        argMap = dict(zip(func_args, args))

        # replace args that are of type ConfigValue with their Config[] value
        newArgs = []
        for arg in args:
            if isinstance(arg, ConfigValue):
                newArgs.append(Config[arg.value])
            else:
                newArgs.append(arg)

        # now do the same for kwargs
        newKwargs = {}

        # get the default args from the function, add them to the kwargs
        sig = inspect.signature(func)
        defaultArgs = sig.parameters
        # now check if the function has any default args that are not in the kwargs
        for k, v in defaultArgs.items():
            if v.default != inspect.Parameter.empty and k not in kwargs and k not in argMap:
                kwargs[k] = v.default

        # replace kwargs that are of type ConfigValue with their Config[] value
        for k, v in kwargs.items():
            if isinstance(v, ConfigValue):
                newKwargs[k] = Config[v.value]
            else:
                newKwargs[k] = v

        return func(*newArgs, **newKwargs)

    return inner
