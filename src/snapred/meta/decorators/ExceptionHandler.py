import functools
import sys
import traceback
from typing import Any, Callable, Type

from snapred.backend.log.logger import snapredLogger

logger = snapredLogger.getLogger(__name__)


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


def ExceptionHandler(exceptionType: Type[Exception]):
    def decorator(func: Callable[..., Any]):
        @functools.wraps(func)
        def inner(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:  # noqa BLE001
                logger.error(f"{extractTrueStacktrace()}")
                raise exceptionType(e)

        return inner

    return decorator
