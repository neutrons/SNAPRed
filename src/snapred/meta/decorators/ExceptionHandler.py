import functools
from typing import Type


def ExceptionHandler(exceptionType: Type[Exception]):
    def decorator(func):
        @functools.wraps(func)
        def inner(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:  # noqa: BLE001
                wrappedException = exceptionType(e)

                if hasattr(wrappedException, "handleStateMessage") and callable(
                    getattr(wrappedException, "handleStateMessage")
                ):
                    wrappedException.handleStateMessage()

                raise wrappedException from e

        return inner

    return decorator
