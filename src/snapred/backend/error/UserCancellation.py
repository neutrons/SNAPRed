from pydantic import BaseModel

from snapred.backend.log.logger import snapredLogger

logger = snapredLogger.getLogger(__name__)


class UserCancellation(Exception):
    """ Raised when cancellation has been requested by the user. """
    
    class Model(BaseModel):
        message: str
 
    def __init__(self, message: str):
        UserCancellation.Model.model_rebuild(force=True)
        self.model = UserCancellation.Model(message=message)
        super().__init__(message)

    @property
    def message(self):
        return self.model.message

    @staticmethod    
    def parse_raw(raw) -> "UserCancellation":
        raw = UserCancellation.Model.model_validate_json(raw)
        return UserCancellation(**raw.dict())
