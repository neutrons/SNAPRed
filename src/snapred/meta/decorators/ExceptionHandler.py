import functools
from typing import Any, Callable, Optional, Type

from snapred.backend.error.RecoverableException import RecoverableException


def ExceptionHandler(exceptionType: Type[Exception], errorMsg: Optional[str] = None):
    def decorator(func: Callable[..., Any]):
        @functools.wraps(func)
        def inner(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:  # noqa BLE001
                if issubclass(exceptionType, RecoverableException) and errorMsg is not None:
                    raise exceptionType(exception=e, errorMsg=errorMsg)
                else:
                    raise exceptionType(e)

        return inner

    return decorator
