import functools
from typing import Type


def ExceptionHandler(exceptionType: Type[Exception]):
    def decorator(func: callable):
        @functools.wraps(func)
        def inner(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:  # noqa: BLE001
                raise exceptionType(e)

        return inner

    return decorator
