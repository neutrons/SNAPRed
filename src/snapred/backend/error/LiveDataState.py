from enum import Enum, auto

from pydantic import BaseModel, model_validator

from snapred.backend.log.logger import snapredLogger

logger = snapredLogger.getLogger(__name__)


class LiveDataState(Exception):
    """
    Raised when a live-data run changes state.
    """

    class Type(Enum):
        UNSET = 0

        # <run number> > 0 <- <run number> == 0
        RUN_START = auto()

        # <run number> == 0 <- <run number> > 0
        RUN_END = auto()

        # <run number>` != <run number>  <- <run number> > 0
        RUN_GAP = auto()

    class Model(BaseModel):
        message: str
        transition: "LiveDataState.Type"

        endRunNumber: str
        startRunNumber: str

        @model_validator(mode="after")
        def _validate_LiveDataState(self):
            if self.endRunNumber == self.startRunNumber or (
                (int(self.endRunNumber) > 0 and int(self.startRunNumber) > 0)
                and int(self.endRunNumber) < int(self.startRunNumber)
            ):
                raise ValueError(f"not a run-state transition: {self.endRunNumber} <- {self.startRunNumber}")
            return self

    def __init__(self, message: str, transition: "Type", endRunNumber: str, startRunNumber: str):
        LiveDataState.Model.model_rebuild(force=True)
        self.model = LiveDataState.Model(
            message=message, transition=transition, endRunNumber=endRunNumber, startRunNumber=startRunNumber
        )
        super().__init__(message)

    @property
    def message(self):
        return self.model.message

    @property
    def transition(self):
        return self.model.transition

    @property
    def endRunNumber(self):
        return self.model.endRunNumber

    @property
    def startRunNumber(self):
        return self.model.startRunNumber

    @staticmethod
    def parse_raw(raw) -> "LiveDataState":
        raw = LiveDataState.Model.model_validate_json(raw)
        return LiveDataState(**raw.dict())

    @staticmethod
    def runStateTransition(endRunNumber: str | int, startRunNumber: str | int) -> "LiveDataState":
        transition = LiveDataState.Type.UNSET
        message = "unknown state transition"
        if int(endRunNumber) > 0 and int(startRunNumber) == 0:
            transition = LiveDataState.Type.RUN_START
            message = f"start of run {endRunNumber}"
        elif int(endRunNumber) == 0 and int(startRunNumber) > 0:
            transition = LiveDataState.Type.RUN_END
            message = f"end of run {startRunNumber}"
        elif int(endRunNumber) > 0 and int(startRunNumber) > 0:
            transition = LiveDataState.Type.RUN_GAP
            message = f"run-number gap: {endRunNumber} <- {startRunNumber}"
        else:
            raise ValueError(f"unexpected run-state transition: {endRunNumber} <- {startRunNumber}")

        return LiveDataState(
            message, transition=transition, endRunNumber=str(endRunNumber), startRunNumber=str(startRunNumber)
        )
