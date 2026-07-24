import functools
import sys
import traceback
from typing import Any, Callable, Tuple, Type

from snapred.backend.log.logger import snapredLogger

logger = snapredLogger.getLogger(__name__)


# Exceptions that genuinely indicate an invalid or inaccessible instrument state (as opposed to a
# programming bug or an unrelated failure). Passed as `ExceptionHandler(StateValidationException,
# convert=STATE_EXCEPTIONS)`, these are the ONLY exceptions re-routed into the domain type; anything
# else propagates as itself.  This way a plain bug (`list.remove(x): x not in list`, a `TypeError`,
# ...) or an unrelated failure (e.g. a live-data read `RuntimeError`) is never mislabeled as
# "Instrument State ... is invalid!".
#
# `OSError` is the umbrella for the file/IO conditions that mean "can't read the state data": it
# covers `FileNotFoundError` and `PermissionError` (both subclasses, and special-cased in
# `StateValidationException`) as well as the plain `OSError` that h5py raises on an unreadable file.
STATE_EXCEPTIONS: Tuple[Type[BaseException], ...] = (OSError,)


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
    convert: Tuple[Type[BaseException], ...] = (),
):
    """
    Decorator that re-routes *selected* exceptions raised by the wrapped function into `exceptionType`.

    This uses an allowlist, the inverse of a blocklist: ONLY the exception types listed in `convert`
    are re-routed into `exceptionType`; every other exception propagates unchanged.  Rather than
    trying (and inevitably failing) to enumerate every exception that must NOT be converted, the
    caller states exactly which ones SHOULD be.  This way genuine bugs and unrelated failures are
    never mislabeled as `exceptionType`.

    Two further guarantees, in contrast to a naive `except SomeType: raise exceptionType(e)`:

    1. The original exception is preserved as the `__cause__` (via `raise ... from e`), so the real
       root cause is never lost, rather than surviving only as a log line.

    2. An exception already of `exceptionType` (or a subclass) is re-raised unchanged rather than
       double-wrapped.

    :param exceptionType: the exception type that listed exceptions are re-routed into.
    :param convert: exception types to re-route into `exceptionType`; all others propagate unchanged.
    """

    def decorator(func: Callable[..., Any]):
        @functools.wraps(func)
        def inner(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except exceptionType:
                # Already the intended type: don't double-wrap.
                raise
            except convert as e:
                logger.error(f"{extractTrueStacktrace()}")
                # Preserve the exception chain so the true cause is not lost.
                raise exceptionType(e) from e

        return inner

    return decorator
