import logging
from enum import Flag, auto
from typing import Any, Optional

from pydantic import BaseModel

from snapred.backend.log.logger import snapredLogger
from snapred.meta.decorators.ExceptionHandler import extractTrueStacktrace

logger = snapredLogger.getLogger(__name__)


class RecoverableException(Exception):
    """
    Raised when a recoverable exception is thrown.
    Allows for custom error types and optional extra context.
    """

    class Type(Flag):
        UNSET = 0
        STATE_UNINITIALIZED = auto()
        CALIBRATION_HOME_WRITE_PERMISSION = auto()

    class Model(BaseModel):
        message: str
        flags: "RecoverableException.Type"
        data: Any

    @property
    def message(self):
        return self.model.message

    @property
    def flags(self):
        return self.model.flags

    @property
    def data(self):
        return self.model.data

    def __init__(self, message: str, flags: "Type" = 0, data: Optional[Any] = None):
        RecoverableException.Model.model_rebuild(force=True)  # replaces: `update_forward_refs` method
        self.model = RecoverableException.Model(message=message, flags=flags, data=data)
        if logger.isEnabledFor(logging.DEBUG):
            # This is a `RecoverableException`, which is barely even an error, so we do NOT spam the logs!
            logger.error(f"{extractTrueStacktrace()}")
        super().__init__(message)

    @staticmethod
    def parse_raw(raw):
        raw = RecoverableException.Model.model_validate_json(raw)
        return RecoverableException(**raw.dict())

    @staticmethod
    def stateUninitialized(runNumber: str, useLiteMode: bool):
        return RecoverableException(
            "State uninitialized",
            flags=RecoverableException.Type.STATE_UNINITIALIZED,
            data={"runNumber": runNumber, "useLiteMode": useLiteMode},
        )
