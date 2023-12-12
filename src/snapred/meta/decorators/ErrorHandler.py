import functools

from snapred.backend.error.StateValidationException import StateValidationException


def StateExceptionHandler(func: callable):
    @functools.wraps(func)
    def inner(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:  # noqa: BLE001
            raise StateValidationException(e)

    return inner
