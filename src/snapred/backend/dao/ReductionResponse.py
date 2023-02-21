from typing import Optional

from pydantic import BaseModel

class ReductionResponse(BaseModel):
    """"""
    responseCode: int
    responseMessage: Optional[str]
    responseData: Optional[dict]

    # if we need specific getter and setter methods, we can use the @property decorator
    # https://docs.python.org/3/library/functions.html#property
    #
    # @property
    # def key(self) -> str:
    #     return self._key

    # @name.setter
    # def key(self, v: str) -> None:
    #     self._key = v