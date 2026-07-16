import functools
import sys
import traceback
from typing import Any, Callable, Tuple, Type

from snapred.backend.log.logger import snapredLogger

logger = snapredLogger.getLogger(__name__)


# Exceptions that indicate a programming error (a bug), not a recoverable domain condition.
# Passing these as `ExceptionHandler(..., passthrough=BUG_EXCEPTIONS)` lets a genuine bug
# propagate as itself instead of being mislabeled as a domain exception (e.g. a plain
# `list.remove(x): x not in list` should not be reported as "Instrument State ... is invalid!").
BUG_EXCEPTIONS: Tuple[Type[BaseException], ...] = (
    TypeError,
    AttributeError,
    NameError,
    IndexError,
    ValueError,
    AssertionError,
)


def extractTrueStacktrace() -> str:
    exc_info = sys.exc_info()
    if exc_info[1] is not None:
        stack = traceback.extract_stack()
        tb = traceback.extract_tb(exc_info[2])
        full_tb = stack[:-1] + tb
        exc_line = traceback.format_exception_only(*exc_info[:2])
        # filter for lines in SNAPREd/tests or SNAPREd/src
        full_tb = [line for line in full_tb if "SNAPRed/tests" in line.filename or "SNAPRed/src" in line.filename]
        stacktraceStr = "\n".join(["".join(traceback.format_list(full_tb)), "".join(exc_line)])
        return stacktraceStr
    return "no exception has occurred"


def ExceptionHandler(
    exceptionType: Type[Exception],
    passthrough: Tuple[Type[BaseException], ...] = (),
):
    """
    Decorator that re-routes exceptions raised by the wrapped function into `exceptionType`.

    Two important guarantees, in contrast to a naive `except Exception: raise exceptionType(e)`:

    1. The original exception is preserved as the `__cause__` (via `raise ... from e`), so the real
       root cause is never lost.  A generic bug (e.g. `list.remove(x): x not in list`) rewrapped into
       a domain exception will still surface its true traceback, rather than surviving only as a log line.

    2. Exceptions that already describe what went wrong are re-raised unchanged rather than mislabeled:
         - `exceptionType` (and its subclasses): already the intended type, so avoid double-wrapping.
         - any type listed in `passthrough`: caller-declared exceptions that must propagate as themselves.

    :param exceptionType: the exception type unexpected exceptions are re-routed into.
    :param passthrough: exception types that should propagate unchanged instead of being re-routed.
    """

    def decorator(func: Callable[..., Any]):
        @functools.wraps(func)
        def inner(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except (exceptionType, *passthrough):
                # Already a meaningful exception: don't re-wrap or mislabel it.
                raise
            except Exception as e:  # noqa BLE001
                logger.error(f"{extractTrueStacktrace()}")
                # Preserve the exception chain so the true cause is not lost.
                raise exceptionType(e) from e

        return inner

    return decorator
