import functools


def EntryExitLogger(logger=None):
    def decorator(func: callable):
        @functools.wraps(func)
        def inner(*args, **kwargs):
            logger.debug(f"Entering {func.__name__}")
            result = func(*args, **kwargs)
            logger.debug(f"Exiting {func.__name__}")
            return result

        return inner

    return decorator
