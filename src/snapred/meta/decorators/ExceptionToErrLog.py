import functools
from typing import Any, Callable, Optional, Type

from snapred.backend.error.RecoverableException import RecoverableException
from snapred.backend.log.logger import snapredLogger

logger = snapredLogger.getLogger(__name__)


def ExceptionToErrLog(func: Callable[..., Any]):
    @functools.wraps(func)
    def inner(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:  # noqa BLE001
            import traceback

            logger.error(e)
            traceback.print_exc()

    return inner
