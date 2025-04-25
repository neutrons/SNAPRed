import functools
import inspect
from typing import Any, Callable, List

def ConfigDefault(**kwargs):
    def decorator(func: Callable[..., Any]):
        
        fullArgSpec: tuple = inspect.getfullargspec(func)
        func_args: List = fullArgSpec.args
        
        @functools.wraps(func)
        def inner(*args, **kwargs):
            return func(*args, **kwargs)
        return inner
    return decorator

