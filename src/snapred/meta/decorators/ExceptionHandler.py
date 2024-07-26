import functools
import sys
import traceback
from typing import Any, Callable, Optional, Type

from snapred.backend.error.RecoverableException import RecoverableException
from snapred.backend.log.logger import snapredLogger

logger = snapredLogger.getLogger(__name__)


def extractTrueStacktrace() -> str:
    exc_info = sys.exc_info()
    stack = traceback.extract_stack()
    tb = traceback.extract_tb(exc_info[2])
    full_tb = stack[:-1] + tb
    exc_line = traceback.format_exception_only(*exc_info[:2])
    stacktraceStr = "\n".join(["".join(traceback.format_list(full_tb)), "".join(exc_line)])
    return stacktraceStr


def ExceptionHandler(exceptionType: Type[Exception], errorMsg: Optional[str] = None):
    def decorator(func: Callable[..., Any]):
        @functools.wraps(func)
        def inner(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:  # noqa BLE001
                logger.error(f"{extractTrueStacktrace()}")
                if issubclass(exceptionType, RecoverableException) and errorMsg is not None:
                    raise exceptionType(exception=e, errorMsg=errorMsg)
                else:
                    raise exceptionType(e)

        return inner

    return decorator
