from dataclasses import dataclass
from snapred.backend.dao.RunConfig import RunConfig

# https://docs.python.org/3/library/dataclasses.html
@dataclass
class ReductionRequest:
    """"""
    mode: str
    runs: list #[RunConfig]

    # if we need specific getter and setter methods, we can use the @property decorator
    # https://docs.python.org/3/library/functions.html#property
    #
    # @property
    # def key(self) -> str:
    #     return self._key

    # @name.setter
    # def key(self, v: str) -> None:
    #     self._key = v