from enum import Flag, auto
from typing import Any, Optional

from pydantic import BaseModel

from snapred.backend.log.logger import snapredLogger

logger = snapredLogger.getLogger(__name__)


class RecoverableException(Exception):
    """
    Raised when a recoverable exception is thrown.
    Allows for custom error types and optional extra context.
    """

    class Type(Flag):
        UNSET = 0
        STATE_UNINITIALIZED = auto()

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
        RecoverableException.Model.update_forward_refs()
        RecoverableException.Model.model_rebuild(force=True)
        self.model = RecoverableException.Model(message=message, flags=flags, data=data)
        super().__init__(message)

    def parse_raw(raw):
        raw = RecoverableException.Model.model_validate_json(raw)
        return RecoverableException(**raw.dict())

    def stateUninitialized(runNumber: str, useLiteMode: bool):
        return RecoverableException(
            "State uninitialized",
            flags=RecoverableException.Type.STATE_UNINITIALIZED,
            data={"runNumber": runNumber, "useLiteMode": useLiteMode},
        )
