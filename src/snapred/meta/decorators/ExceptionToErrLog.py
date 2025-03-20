import functools
import logging
from typing import Any, Callable

from snapred.backend.log.logger import snapredLogger

logger = snapredLogger.getLogger(__name__)


def ExceptionToErrLog(func: Callable[..., Any]):
    @functools.wraps(func)
    def inner(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:  # noqa BLE001
            logger.error(e)
            if logger.isEnabledFor(logging.DEBUG):
                # print stacktrace
                import traceback

                traceback.print_exc()

    return inner
