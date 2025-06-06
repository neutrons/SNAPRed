import functools
import inspect
from typing import Any, Callable, List

from snapred.meta.Config import Config


class ConfigValue:
    def __init__(self, value: Any):
        self.value = value

    def get(self):
        return Config[self.value]


def ConfigDefault(func: Callable[..., Any]):
    """
    This decorator allows methods to evaluate default args set to Config[] at "execution" time.
    Where as the normal behavior of Python is to evaluate the default args at "compile" time.
    This allows reloading of Config values without having to restart the program.
    But maintains the pythonic style of using default args.
    """

    @functools.wraps(func)
    def inner(*args, **kwargs):
        # 1. Flatten args into their Config[] values
        # 2. Collect unspecified kwargs which have default values
        # 3. If said default values are ConfigValues, replace them with their Config[] value
        # 4. Call the function with the new args and kwargs
        fullArgSpec: tuple = inspect.getfullargspec(func)
        func_args: List = fullArgSpec.args
        # map args to func_args
        argMap = dict(zip(func_args, args))

        # replace args that are of type ConfigValue with their Config[] value
        newArgs = []
        for arg in args:
            if isinstance(arg, ConfigValue):
                newArgs.append(arg.get())
            else:
                newArgs.append(arg)

        # now do the same for kwargs
        newKwargs = {}

        # get the default args from the function, add them to the kwargs
        sig = inspect.signature(func)
        defaultArgs = sig.parameters
        # now check if the function has any default args that are not in the kwargs
        for k, v in defaultArgs.items():
            # If it has a default and was not specified in neither kwarg nor arg
            # then we need to inspect if it is a ConfigValue later
            if v.default != inspect.Parameter.empty and k not in kwargs and k not in argMap:
                kwargs[k] = v.default

        # replace kwargs that are of type ConfigValue with their Config[] value
        for k, v in kwargs.items():
            if isinstance(v, ConfigValue):
                newKwargs[k] = v.get()
            else:
                newKwargs[k] = v

        return func(*newArgs, **newKwargs)

    return inner
