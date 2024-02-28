import functools
from typing import Any, Callable, Optional, Type

from snapred.backend.error.RecoverableException import RecoverableException


def ExceptionHandler(exceptionType: Type[Exception], errorType: Optional[str] = None):
    def decorator(func: Callable[..., Any]):
        @functools.wraps(func)
        def inner(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:  # noqa BLE001
                if issubclass(exceptionType, RecoverableException) and errorType is not None:
                    raise exceptionType(exception=e, errorType=errorType)
                else:
                    raise exceptionType(e)

        return inner

    return decorator
